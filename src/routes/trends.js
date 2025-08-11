// src/routes/trends.js
const express = require('express');
const Trend = require('../models/trendModel');
const config = require('../config');
const IORedis = require('ioredis');
const redis = new IORedis(config.redis);
const router = express.Router();
const winston = require('winston');
const { get } = require('lodash');

const logger = winston.createLogger({ transports: [new winston.transports.Console()] });

const { fetchAndProcess } = require('../services/gdeltFetcher');
const { scoreTrends } = require('../services/trendScorer');

async function getCachedOrDb(key, dbQuery) {
    const cached = await redis.get(key);
    if (cached) return JSON.parse(cached);
    const doc = await dbQuery();
    if (doc) {
        await redis.set(key, JSON.stringify(doc), 'EX', 900);
    }
    return doc;
}

function parseWindowDays(input) {
    if (!input) return 7;
    const raw = String(input).trim().toLowerCase();
    if (/^\d+$/.test(raw)) return Math.max(1, parseInt(raw, 10));
    const m = raw.match(/^(\d+)\s*([dmy])$/);
    if (m) {
        const n = parseInt(m[1], 10);
        const unit = m[2];
        if (unit === 'd') return Math.max(1, n);
        if (unit === 'm') return Math.max(1, n * 30);
        if (unit === 'y') return Math.max(1, n * 365);
    }
    const presets = {
        '7d': 7,
        '30d': 30,
        '3m': 90,
        '1y': 365,
        '3y': 365 * 3,
    };
    if (raw in presets) return presets[raw];
    return 7;
}

// GET /trends/realtime?date=YYYY-MM-DD&category=themes|persons|orgs|documents|all
router.get('/realtime', async (req, res) => {
    logger.info(`Received /realtime request. Query: ${JSON.stringify(req.query)}`);
    try {
        const date = req.query.date || new Date().toISOString().slice(0, 10);
        const category = req.query.category || 'all';
        const key = `realtime:${date}:${category}`;
        const queryCategory = category === 'all' ? { $in: ['themes', 'persons', 'orgs', 'documents'] } : category;

        const docs = await getCachedOrDb(key, () =>
            Trend.find({ type: 'realtime', date, category: queryCategory })
                .sort({ timestamp: -1 })
                .limit(20)
                .lean()
                .exec()
        );

        logger.info(`Sending /realtime response. Date: ${date}, Category: ${category}, Results count: ${docs ? docs.length : 0}`);
        return res.json({ date, category, results: docs });
    } catch (err) {
        logger.error(`Error in /realtime: ${err.message}`);
        return res.status(500).json({ error: err.message });
    }
});

// GET /trends/daily?date=YYYY-MM-DD&category=themes|persons|orgs|documents|all
router.get('/daily', async (req, res) => {
    logger.info(`Received /daily request. Query: ${JSON.stringify(req.query)}`);
    try {
        const date = req.query.date || new Date().toISOString().slice(0, 10);
        const category = req.query.category || 'all';
        const key = `daily:${date}:${category}`;

        const docs = await getCachedOrDb(key, () => {
            if (category === 'all') {
                return Trend.find({ type: 'daily', date }).lean().exec();
            } else {
                return Trend.findOne({ type: 'daily', date, category }).lean().exec();
            }
        });

        const categories = Array.isArray(docs) ? docs.map(d => d.category) : (docs ? [docs.category] : []);
        logger.info(`Sending /daily response. Date: ${date}, Category: ${category}, Results: ${docs ? (Array.isArray(docs) ? docs.length : 1) : 0}, Categories present: ${categories.join(',')}`);
        return res.json({ date, category, results: docs });
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

        await Promise.all([
            redis.del(`daily:${dateStr}:all`),
            redis.del(`daily:${dateStr}:themes`),
            redis.del(`daily:${dateStr}:persons`),
            redis.del(`daily:${dateStr}:orgs`),
            redis.del(`daily:${dateStr}:documents`),
        ]);
        return res.json({ ok });
    } catch (err) {
        logger.error(`Admin fetchDaily error: ${err.message}`);
        return res.status(500).json({ error: err.message });
    }
});

// GET /trends/top?date=YYYY-MM-DD&category=themes|persons|orgs|documents&window=7|30|3m|1y|3y&limit=50
router.get('/top', async (req, res) => {
    logger.info(`Received /top request. Query: ${JSON.stringify(req.query)}`);
    try {
        const date = req.query.date || new Date().toISOString().slice(0, 10);
        const category = req.query.category || 'themes';
        const windowParam = req.query.window || req.query.range || '7d';
        const windowDays = parseWindowDays(windowParam);
        const limit = parseInt(req.query.limit || '50', 10);
        const noCache = req.query.nocache === '1';
        const cacheKey = `top:${date}:${category}:${windowDays}:${limit}`;

        if (!noCache) {
            const cached = await redis.get(cacheKey);
            if (cached) {
                logger.debug(`[CACHE HIT] /top → key: ${cacheKey}`);
                return res.json(JSON.parse(cached));
            } else {
                logger.debug(`[CACHE MISS] /top → key: ${cacheKey}`);
            }
        } else {
            logger.debug(`[CACHE BYPASSED] /top → key: ${cacheKey}`);
        }

        const ranked = await scoreTrends({ date, category, windowDays, topN: limit });
        const payload = { date, category, window: windowDays, results: ranked || [] };

        if (!noCache) {
            await redis.set(cacheKey, JSON.stringify(payload), 'EX', 600);
            logger.debug(`[CACHE STORE] /top → key: ${cacheKey} stored for 600s`);
        }

        return res.json(payload);
    } catch (err) {
        logger.error(`Error in /top: ${err.message}`);
        return res.status(500).json({ error: err.message });
    }
});

// GET /trends/documents
router.get('/documents', async (req, res) => {
    logger.info(`Received /documents request. Query: ${JSON.stringify(req.query)}`);
    try {
        const date = req.query.date || new Date().toISOString().slice(0, 10);
        const key = `documents:${date}`;
        const doc = await getCachedOrDb(key, () =>
            Trend.findOne({ type: 'daily', date, category: 'documents' }).lean().exec()
        );

        const documentIdentifiers = (doc && doc.keywords) ? doc.keywords.map(k => k.word) : [];
        logger.info(`Sending /documents response. Date: ${date}, Results count: ${documentIdentifiers.length}`);
        return res.json({ date, results: documentIdentifiers });
    } catch (err) {
        logger.error(`Error in /documents: ${err.message}`);
        return res.status(500).json({ error: err.message });
    }
});

module.exports = router;
