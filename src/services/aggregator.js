// src/services/aggregator.js
const Trend = require('../models/trendModel');
const { rankByCount } = require('../utils/ranker');
const mongoose = require('mongoose');
const config = require('../config');
const IORedis = require('ioredis');
const redis = new IORedis(config.redis);
const winston = require('winston');

const logger = winston.createLogger({ transports: [new winston.transports.Console()] });

// store aggregated result into DB and cache
async function aggregateFromFile({ collector, timestamp = new Date(), category = 'all' }) {
    // collector: { themes: [], persons: [], orgs: [] }
    // timestamp: JS Date
    const dateStr = timestamp.toISOString().slice(0, 10);

    // For each category (or selected)
    const tasks = [];
    const topN = config.topN || 50;

    const categoriesToProcess = (category === 'all') ? ['themes', 'persons', 'orgs'] : [category];

    for (const cat of categoriesToProcess) {
        const arr = (collector[cat] || []);
        const ranked = rankByCount(arr, topN);

        const doc = {
            timestamp,
            type: 'realtime',
            date: dateStr,
            category: cat,
            keywords: ranked
        };

        // upsert a realtime doc for this date+time+category
        const filter = { type: 'realtime', date: dateStr, category: cat, timestamp: timestamp };
        tasks.push(Trend.findOneAndUpdate(filter, doc, { upsert: true, new: true, setDefaultsOnInsert: true }).exec());
        // cache key
        const cacheKey = `realtime:${dateStr}:${cat}`;
        tasks.push(redis.set(cacheKey, JSON.stringify({ timestamp, date: dateStr, type: 'realtime', category: cat, keywords: ranked }), 'EX', (config.realtimeIntervalMin || 15) * 60));
    }

    await Promise.all(tasks);
    logger.info(`Aggregated file results for ${dateStr}`);
    return true;
}

// daily aggregation: take many collectors / or pass array of collectors
async function aggregateDaily({ collectorsArray, date /* YYYY-MM-DD */, category = 'all' }) {
    // collectorsArray: array of collector objects { themes: [], persons: [], orgs: [] } from many files
    const topN = config.topN || 50;
    const merged = { themes: [], persons: [], orgs: [] };
    for (const c of collectorsArray) {
        merged.themes.push(...(c.themes || []));
        merged.persons.push(...(c.persons || []));
        merged.orgs.push(...(c.orgs || []));
    }

    const categories = (category === 'all') ? ['themes', 'persons', 'orgs'] : [category];
    const timestamp = new Date();

    for (const cat of categories) {
        const arr = merged[cat] || [];
        const ranked = rankByCount(arr, topN);

        const doc = {
            timestamp,
            type: 'daily',
            date: date,
            category: cat,
            keywords: ranked
        };

        await Trend.findOneAndUpdate({ type: 'daily', date, category: cat }, doc, { upsert: true, new: true, setDefaultsOnInsert: true }).exec();

        const cacheKey = `daily:${date}:${cat}`;
        await redis.set(cacheKey, JSON.stringify({ timestamp, date, type: 'daily', category: cat, keywords: ranked }), 'EX', 24 * 3600);
    }
    logger.info(`Daily aggregated for ${date}`);
}

module.exports = { aggregateFromFile, aggregateDaily };
