// src/services/gdeltFetcher.js
const axios = require('axios');
const unzipper = require('unzipper');
const { get } = require('lodash');
const config = require('../config');
const Trend = require('../models/trendModel');
const winston = require('winston');
const csv = require('fast-csv');
const { splitAndClean } = require('../utils/cleaner');
const { rankByCount } = require('../utils/ranker');

const logger = winston.createLogger({ transports: [new winston.transports.Console()] });

function getFilenameForUTC(date) {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');
    const hours = String(date.getUTCHours()).padStart(2, '0');
    const minutes = String(Math.floor(date.getUTCMinutes() / 15) * 15).padStart(2, '0');
    return `${year}${month}${day}${hours}${minutes}00.gkg.csv.zip`;
}

function getDailyFilenameForUTC(date) {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');
    return `${year}${month}${day}.gkg.csv.zip`;
}

async function parseCsvStreamToCollector(stream) {
    const collector = { themes: [], persons: [], orgs: [] };
    let headerDetected = false;

    // Set sane defaults if not provided (common GKG v2 ordering)
    if (config.columnIndices.themes === null || config.columnIndices.themes === undefined) {
        config.columnIndices.themes = 7; // V2Themes
    }
    if (config.columnIndices.persons === null || config.columnIndices.persons === undefined) {
        config.columnIndices.persons = 9; // V2Persons
    }
    if (config.columnIndices.orgs === null || config.columnIndices.orgs === undefined) {
        config.columnIndices.orgs = 10; // V2Organizations
    }
    logger.info(`Using column indices -> themes:${config.columnIndices.themes}, persons:${config.columnIndices.persons}, orgs:${config.columnIndices.orgs}`);

    return new Promise((resolve, reject) => {
        const parserStream = csv.parse({ headers: false, relax_quotes: true, trim: true, delimiter: '\t' })
            .on('error', err => reject(err))
            .on('data', row => {
                try {
                    if (!headerDetected) {
                        const rowStr = row.join('|').toLowerCase();
                        if (rowStr.includes('v2themes') || rowStr.includes('v2persons') || rowStr.includes('v2organizations')) {
                            headerDetected = true;
                            const header = row.map(c => String(c).toLowerCase());
                            const th = header.findIndex(h => h.includes('v2themes'));
                            if (th >= 0) config.columnIndices.themes = th;
                            const pe = header.findIndex(h => h.includes('v2persons'));
                            if (pe >= 0) config.columnIndices.persons = pe;
                            const or = header.findIndex(h => h.includes('v2organizations'));
                            if (or >= 0) config.columnIndices.orgs = or;
                            logger.info(`Header detected, updated indices -> themes:${config.columnIndices.themes}, persons:${config.columnIndices.persons}, orgs:${config.columnIndices.orgs}`);
                            return; // skip header
                        }
                    }
                    const getCol = (idx) => (idx !== null && idx !== undefined && row[idx] !== undefined) ? row[idx] : null;
                    const rawThemes = getCol(config.columnIndices.themes);
                    if (rawThemes) collector.themes.push(...splitAndClean(rawThemes));
                    const rawPersons = getCol(config.columnIndices.persons);
                    if (rawPersons) collector.persons.push(...splitAndClean(rawPersons));
                    const rawOrgs = getCol(config.columnIndices.orgs);
                    if (rawOrgs) collector.orgs.push(...splitAndClean(rawOrgs));
                } catch (e) {
                    // continue on row errors
                }
            })
            .on('end', () => resolve(collector));
        stream.pipe(parserStream);
    });
}

async function downloadAndParse(url) {
    logger.info(`Attempting to fetch data from GDELT: ${url}`);
    const resp = await axios({ url, method: 'GET', responseType: 'stream', timeout: 300000 });
    const unzipStream = resp.data.pipe(unzipper.ParseOne());
    return parseCsvStreamToCollector(unzipStream);
}

function rankCollector(collector) {
    const topN = config.topN || 50;
    return {
        themes: rankByCount(collector.themes || [], topN),
        persons: rankByCount(collector.persons || [], topN),
        orgs: rankByCount(collector.orgs || [], topN),
    };
}

async function fetchAndProcess(date, options = {}) {
    const timestamp = options.timestamp || date;

    // Try 15-min file
    const fifteenName = getFilenameForUTC(date);
    const fifteenUrl = `${config.gdeltBaseUrl}/${fifteenName}`;

    try {
        const collector = await downloadAndParse(fifteenUrl);
        const ranked = rankCollector(collector);
        logger.info(`Parsed 15-min file ${fifteenName}. Themes: ${collector.themes.length}, Persons: ${collector.persons.length}, Orgs: ${collector.orgs.length}. Ranked -> T:${ranked.themes.length} P:${ranked.persons.length} O:${ranked.orgs.length}`);
        await saveTrends({ date, timestamp, jobType: options.jobType || 'realtime', ...ranked });
        return true;
    } catch (err) {
        const status = err.response && err.response.status;
        logger.warn(`15-min fetch failed (${status || 'no status'}): ${err.message}. Falling back to daily.`);
    }

    // Fallback to daily: today, then yesterday
    for (const offsetDays of [0, 1]) {
        const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate() - offsetDays));
        const dailyName = getDailyFilenameForUTC(d);
        const dailyUrl = `${config.gdeltDailyBaseUrl}/${dailyName}`;
        try {
            const collector = await downloadAndParse(dailyUrl);
            const ranked = rankCollector(collector);
            logger.info(`Parsed daily file ${dailyName}. Themes: ${collector.themes.length}, Persons: ${collector.persons.length}, Orgs: ${collector.orgs.length}. Ranked -> T:${ranked.themes.length} P:${ranked.persons.length} O:${ranked.orgs.length}`);
            const dailyDate = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
            await saveTrends({ date: dailyDate, timestamp: dailyDate, jobType: 'daily', ...ranked });
            return true;
        } catch (err) {
            const status = err.response && err.response.status;
            logger.warn(`Daily fetch failed for ${dailyName} (${status || 'no status'}): ${err.message}`);
        }
    }

    logger.error('All fetch attempts failed (15-min and daily fallbacks).');
    return false;
}

async function saveTrends({ date, timestamp, jobType, themes, persons, orgs }) {
    const isoDate = new Date(date).toISOString().slice(0, 10);
    const trends = [
        { type: jobType || 'realtime', date: isoDate, category: 'themes', keywords: themes, timestamp },
        { type: jobType || 'realtime', date: isoDate, category: 'persons', keywords: persons, timestamp },
        { type: jobType || 'realtime', date: isoDate, category: 'orgs', keywords: orgs, timestamp },
    ];

    for (const trendData of trends) {
        if (trendData.keywords && trendData.keywords.length > 0) {
            await Trend.findOneAndUpdate(
                { type: trendData.type, date: trendData.date, category: trendData.category },
                { $set: { keywords: trendData.keywords, timestamp: trendData.timestamp } },
                { upsert: true, new: true }
            );
            logger.info(`Saved/Updated ${trendData.type} ${trendData.category} trends for ${trendData.date}`);
        } else {
            logger.info(`No keywords for ${trendData.type} ${trendData.category} on ${trendData.date}`);
        }
    }
}

module.exports = { fetchAndProcess, getFilenameForUTC, getDailyFilenameForUTC };
