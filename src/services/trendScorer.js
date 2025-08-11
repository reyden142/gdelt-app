// src/services/trendScorer.js
const Trend = require('../models/trendModel');
const { filterNoiseKeywords, isNumericVector, isNoiseToken } = require('../utils/cleaner');
const { fetchAndProcess } = require('./gdeltFetcher');
const winston = require('winston');

const logger = winston.createLogger({ transports: [new winston.transports.Console()] });

function normalizeTo100(values) {
    if (!values.length) return [];
    const max = Math.max(...values);
    if (max <= 0) return values.map(() => 0);
    return values.map(v => Math.round((v / max) * 100));
}

function computeStats(arr) {
    const n = arr.length || 1;
    const mean = arr.reduce((a, b) => a + b, 0) / n;
    const variance = arr.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / n;
    const std = Math.sqrt(variance);
    return { mean, std };
}

function mergeCounts(docs) {
    const map = new Map();
    for (const doc of docs) {
        for (const k of (doc.keywords || [])) {
            if (!k || !k.word) continue;
            map.set(k.word, (map.get(k.word) || 0) + (k.count || 0));
        }
    }
    return map;
}

function toDate(dstr) {
    return new Date(`${dstr}T12:00:00.000Z`);
}

function isoDay(d) {
    return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate())).toISOString().slice(0, 10);
}

function generateWindowDates(dateStr, windowDays) {
    const dates = [];
    const end = toDate(dateStr);
    const start = new Date(end);
    start.setUTCDate(start.getUTCDate() - windowDays);
    for (let d = new Date(start); d < end; d.setUTCDate(d.getUTCDate() + 1)) {
        dates.push(isoDay(d));
    }
    return dates; // excludes current dateStr
}

async function ensureDailyCoverage(dateStr, windowDays) {
    const allNeeded = new Set([dateStr, ...generateWindowDates(dateStr, windowDays)]);
    const existing = await Trend.find({ type: 'daily', date: { $in: Array.from(allNeeded) } }, { date: 1 }).lean();
    const have = new Set(existing.map(d => d.date));
    const missing = Array.from(allNeeded).filter(d => !have.has(d));
    if (missing.length === 0) return;

    const softAwaitLimit = 31;
    const toAwait = missing.slice(0, softAwaitLimit);
    const toBackground = missing.slice(softAwaitLimit);

    if (toAwait.length) {
        logger.info(`Ensuring daily coverage (await) for ${toAwait.length} days`);
        await Promise.allSettled(toAwait.map(ds => fetchAndProcess(toDate(ds), { jobType: 'daily', timestamp: toDate(ds) })));
    }
    if (toBackground.length) {
        logger.info(`Ensuring daily coverage (background) for ${toBackground.length} days`);
        // Fire-and-forget; don't await large ranges
        toBackground.forEach(ds => fetchAndProcess(toDate(ds), { jobType: 'daily', timestamp: toDate(ds) }).catch(() => { }));

    }
}

function scoreCore({ current, baselineMap, windowDays, topN }) {
    const scores = [];
    const baselineValues = Array.from(baselineMap.values());
    const { mean, std } = computeStats(baselineValues.length ? baselineValues : [0]);

    for (const { word, count } of current) {
        const base = baselineMap.get(word) || 0;
        const volume = Math.log1p(count);
        const growth = (count + 1) / (base / Math.max(windowDays, 1) + 1);
        const z = std > 0 ? (count - mean) / std : 0;
        const rawScore = 0.6 * volume + 0.3 * Math.log1p(growth) + 0.1 * Math.max(0, z);
        scores.push({ word, rawScore });
    }

    const normalized = normalizeTo100(scores.map(s => s.rawScore));
    return scores
        .map((s, i) => ({ word: s.word, score: normalized[i] }))
        .sort((a, b) => b.score - a.score)
        .slice(0, topN);
}

async function scoreTrends({ date, category = 'themes', windowDays = 7, topN = 50 }) {
    // Ensure we have daily docs for current date and baseline window
    await ensureDailyCoverage(date, windowDays);

    const targetDate = new Date(`${date}T00:00:00.000Z`);
    const startDate = new Date(targetDate);
    startDate.setUTCDate(startDate.getUTCDate() - windowDays);

    const [currentDoc, baselineDocs] = await Promise.all([
        Trend.findOne({ type: 'daily', date, category }).lean().exec(),
        Trend.find({ type: 'daily', date: { $gte: startDate.toISOString().slice(0, 10), $lt: date }, category }).lean().exec()
    ]);

    if (!currentDoc || !currentDoc.keywords || currentDoc.keywords.length === 0) {
        logger.info(`No current daily doc for ${date} ${category}`);
        return [];
    }

    let usedCurrent = filterNoiseKeywords(currentDoc.keywords);
    const baselineMap = mergeCounts(baselineDocs.map(d => ({ keywords: filterNoiseKeywords(d.keywords || []) })));

    let ranked = scoreCore({ current: usedCurrent, baselineMap, windowDays, topN });

    // If too aggressive filtering yielded nothing, retry with a looser filter (drop only numeric vectors)
    if (ranked.length === 0) {
        usedCurrent = (currentDoc.keywords || []).filter(k => k && k.word && !isNumericVector(k.word));
        const baselineLoose = mergeCounts(baselineDocs.map(d => ({ keywords: (d.keywords || []).filter(k => k && k.word && !isNumericVector(k.word)) })));
        ranked = scoreCore({ current: usedCurrent, baselineMap: baselineLoose, windowDays, topN });
        logger.info(`Loose scoring used for ${date} ${category} (results: ${ranked.length})`);
    }

    // Last resort: if still empty, take top counts by volume only from current day (still exclude obvious noise)
    if (ranked.length === 0) {
        usedCurrent = (currentDoc.keywords || []).filter(k => k && k.word && !isNoiseToken(k.word));
        const fallback = usedCurrent
            .slice() // copy
            .sort((a, b) => b.count - a.count)
            .slice(0, topN)
            .map(k => ({ word: k.word, score: 100 }));
        logger.info(`Fallback volume-only ranking used for ${date} ${category} (results: ${fallback.length})`);
        ranked = fallback;
    }

    // Build a count map from the current set used in scoring and enrich the returned objects
    const currentCountMap = new Map();
    for (const k of usedCurrent) {
        if (!k || !k.word) continue;
        currentCountMap.set(k.word, (currentCountMap.get(k.word) || 0) + (k.count || 0));
    }

    const rankedWithCounts = ranked.map(r => ({ ...r, count: currentCountMap.get(r.word) || 0 }));

    const resultDoc = {
        timestamp: new Date(),
        type: 'ranked',
        date,
        category,
        keywords: rankedWithCounts.map(k => ({ word: k.word, count: k.count, score: k.score }))
    };

    await Trend.findOneAndUpdate({ type: 'ranked', date, category }, resultDoc, { upsert: true, new: true, setDefaultsOnInsert: true }).exec();
    logger.info(`Ranked trends saved for ${date} ${category} (top ${ranked.length})`);
    return rankedWithCounts;

}

module.exports = { scoreTrends }; 