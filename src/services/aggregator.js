// src/services/aggregator.js
const Trend = require('../models/trendModel');
const mongoose = require('mongoose');
const config = require('../config');
const IORedis = require('ioredis');
const redis = new IORedis(config.redis);
const winston = require('winston');

const logger = winston.createLogger({ transports: [new winston.transports.Console()] });

/**
 * rankByCount: ranks keywords by total count, merges document arrays.
 * Input: array of {word, count, documents?}
 * Output: top N array of {word, count, documents}
 */
function rankByCount(arr, topN) {
    const map = new Map();

    for (const item of arr) {
        const w = item.word.toLowerCase();
        if (!map.has(w)) {
            map.set(w, { word: w, count: 0, documents: new Set() });
        }
        const entry = map.get(w);
        entry.count += item.count || 1;
        if (Array.isArray(item.documents)) {
            for (const doc of item.documents) {
                entry.documents.add(doc);
            }
        }
    }

    // Convert sets to arrays and sort descending by count
    const ranked = Array.from(map.values())
        .map(({ word, count, documents }) => ({
            word,
            count,
            documents: Array.from(documents),
        }))
        .sort((a, b) => b.count - a.count)
        .slice(0, topN);

    return ranked;
}

// store aggregated result into DB and cache
async function aggregateFromFile({ collector, timestamp = new Date(), category = 'all' }) {
    const dateStr = timestamp.toISOString().slice(0, 10);
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

        const filter = { type: 'realtime', date: dateStr, category: cat, timestamp };
        tasks.push(Trend.findOneAndUpdate(filter, doc, { upsert: true, new: true, setDefaultsOnInsert: true }).exec());

        const cacheKey = `realtime:${dateStr}:${cat}`;
        tasks.push(redis.set(cacheKey, JSON.stringify({ timestamp, date: dateStr, type: 'realtime', category: cat, keywords: ranked }), 'EX', (config.realtimeIntervalMin || 15) * 60));
    }

    // Also save document identifiers separately (just unique list with count 1)
    const docIdsUnique = Array.from(new Set(collector.documentIdentifiers || []));
    if (docIdsUnique.length > 0) {
        const docIdsDoc = {
            timestamp,
            type: 'realtime',
            date: dateStr,
            category: 'documents',
            keywords: docIdsUnique.map(id => ({ word: id, count: 1 })),
        };
        const filter = { type: 'realtime', date: dateStr, category: 'documents', timestamp };
        tasks.push(Trend.findOneAndUpdate(filter, docIdsDoc, { upsert: true, new: true, setDefaultsOnInsert: true }).exec());
        logger.info(`Aggregated ${docIdsUnique.length} document identifiers for ${dateStr}.`);
    }

    await Promise.all(tasks);
    logger.info(`Aggregated file results for ${dateStr}`);
    return true;
}

// daily aggregation remains same, update to use new rankByCount too
async function aggregateDaily({ collectorsArray, date /* YYYY-MM-DD */, category = 'all' }) {
    const topN = config.topN || 50;
    const merged = { themes: [], persons: [], orgs: [], documentIdentifiers: [] };
    for (const c of collectorsArray) {
        merged.themes.push(...(c.themes || []));
        merged.persons.push(...(c.persons || []));
        merged.orgs.push(...(c.orgs || []));
        merged.documentIdentifiers.push(...(c.documentIdentifiers || []));
    }

    const categories = (category === 'all') ? ['themes', 'persons', 'orgs'] : [category];
    const timestamp = new Date();

    const tasks = [];
    for (const cat of categories) {
        const arr = merged[cat] || [];
        const ranked = rankByCount(arr, topN);

        const doc = {
            timestamp,
            type: 'daily',
            date,
            category: cat,
            keywords: ranked
        };

        tasks.push(Trend.findOneAndUpdate({ type: 'daily', date, category: cat }, doc, { upsert: true, new: true, setDefaultsOnInsert: true }).exec());

        const cacheKey = `daily:${date}:${cat}`;
        tasks.push(redis.set(cacheKey, JSON.stringify({ timestamp, date, type: 'daily', category: cat, keywords: ranked }), 'EX', 24 * 3600));
    }

    // Save daily document identifiers separately
    const docIdsUnique = Array.from(new Set(merged.documentIdentifiers));
    if (docIdsUnique.length > 0) {
        const docIdsDoc = {
            timestamp,
            type: 'daily',
            date,
            category: 'documents',
            keywords: docIdsUnique.map(id => ({ word: id, count: 1 })),
        };
        tasks.push(Trend.findOneAndUpdate({ type: 'daily', date, category: 'documents' }, docIdsDoc, { upsert: true, new: true, setDefaultsOnInsert: true }).exec());
        logger.info(`Aggregated ${docIdsUnique.length} daily document identifiers for ${date}.`);
    }

    await Promise.all(tasks);
    logger.info(`Daily aggregated for ${date}`);
    return true;
}

module.exports = { aggregateFromFile, aggregateDaily, rankByCount };
