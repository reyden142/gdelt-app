// src/routes/trends.js
const express = require('express');
const Trend = require('../models/trendModel');
const config = require('../config');
const IORedis = require('ioredis');
const redis = new IORedis(config.redis);
const router = express.Router();
const winston = require('winston');

const logger = winston.createLogger({ transports: [new winston.transports.Console()] });

const { fetchAndProcess } = require('../services/gdeltFetcher');

async function getCachedOrDb(key, dbQuery) {
    const cached = await redis.get(key);
    if (cached) return JSON.parse(cached);
    const doc = await dbQuery();
    if (doc) {
        await redis.set(key, JSON.stringify(doc), 'EX', 900);
    }
    return doc;
}

// GET /trends/realtime?date=YYYY-MM-DD&category=themes|persons|orgs
router.get('/realtime', async (req, res) => {
    logger.info(`Received /realtime request. Query: ${JSON.stringify(req.query)}`);
    try {
        const date = req.query.date || new Date().toISOString().slice(0, 10);
        const category = req.query.category || 'all';
        const key = `realtime:${date}:${category}`;
        const doc = await getCachedOrDb(key, () => Trend.find({ type: 'realtime', date, category: category === 'all' ? { $in: ['themes', 'persons', 'orgs'] } : category }).sort({ timestamp: -1 }).limit(20).lean().exec());
        logger.info(`Sending /realtime response. Date: ${date}, Category: ${category}, Results count: ${doc ? doc.length : 0}`);
        return res.json({ date, category, results: doc });
    } catch (err) {
        logger.error(`Error in /realtime: ${err.message}`);
        return res.status(500).json({ error: err.message });
    }
});

// GET /trends/daily?date=YYYY-MM-DD&category=themes|persons|orgs
router.get('/daily', async (req, res) => {
    logger.info(`Received /daily request. Query: ${JSON.stringify(req.query)}`);
    try {
        const date = req.query.date || new Date().toISOString().slice(0, 10);
        const category = req.query.category || 'all';
        const key = `daily:${date}:${category}`;
        const doc = await getCachedOrDb(key, () => {
            if (category === 'all') {
                return Trend.find({ type: 'daily', date }).lean().exec();
            } else {
                return Trend.findOne({ type: 'daily', date, category }).lean().exec();
            }
        });
        const categories = Array.isArray(doc) ? doc.map(d => d.category) : (doc ? [doc.category] : []);
        logger.info(`Sending /daily response. Date: ${date}, Category: ${category}, Results: ${doc ? (Array.isArray(doc) ? doc.length : 1) : 0}, Categories present: ${categories.join(',')}`);
        return res.json({ date, category, results: doc });
    } catch (err) {
        logger.error(`Error in /daily: ${err.message}`);
        return res.status(500).json({ error: err.message });
    }
});

// POST /trends/admin/fetchDaily?date=YYYY-MM-DD
router.post('/admin/fetchDaily', async (req, res) => {
    const dateStr = req.query.date;
    if (!dateStr || !/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
        return res.status(400).json({ error: 'date query param required: YYYY-MM-DD' });
    }
    try {
        const d = new Date(`${dateStr}T12:00:00.000Z`);
        logger.info(`Admin: manual daily fetch for ${dateStr}`);
        const ok = await fetchAndProcess(d, { category: 'all', timestamp: d, jobType: 'daily' });
        // Clear caches for this date so fresh query returns updated data
        await Promise.all([
            redis.del(`daily:${dateStr}:all`),
            redis.del(`daily:${dateStr}:themes`),
            redis.del(`daily:${dateStr}:persons`),
            redis.del(`daily:${dateStr}:orgs`),
        ]);
        return res.json({ ok });
    } catch (err) {
        logger.error(`Admin fetchDaily error: ${err.message}`);
        return res.status(500).json({ error: err.message });
    }
});

module.exports = router;
