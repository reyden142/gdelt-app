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
    const collector = { themes: [], persons: [], orgs: [], documentIdentifiers: [] };
    let headerDetected = false;
    let rowCount = 0;

    // Set sane defaults if not provided
    if (config.columnIndices.themes === null || config.columnIndices.themes === undefined) {
        config.columnIndices.themes = 7; // V2Themes
    }
    if (config.columnIndices.persons === null || config.columnIndices.persons === undefined) {
        config.columnIndices.persons = 9; // V2Persons
    }
    if (config.columnIndices.orgs === null || config.columnIndices.orgs === undefined) {
        config.columnIndices.orgs = 10; // V2Organizations
    }
    // Add default index for DocumentIdentifier
    if (config.columnIndices.documentIdentifier === null || config.columnIndices.documentIdentifier === undefined) {
        config.columnIndices.documentIdentifier = 4; // DocumentIdentifier
    }
    logger.info(`Using column indices -> themes:${config.columnIndices.themes}, persons:${config.columnIndices.persons}, orgs:${config.columnIndices.orgs}, docId:${config.columnIndices.documentIdentifier}`);

    return new Promise((resolve, reject) => {
        const parserStream = csv.parse({ headers: false, relax_quotes: true, trim: true, delimiter: '\t' })
            .on('error', err => reject(err))
            .on('data', row => {
                rowCount++;
                try {
                    if (!headerDetected) {
                        const rowStr = row.join('|').toLowerCase();
                        if (rowStr.includes('v2themes') || rowStr.includes('v2persons') || rowStr.includes('v2organizations') || rowStr.includes('documentidentifier')) {
                            headerDetected = true;
                            const header = row.map(c => String(c).toLowerCase());
                            const th = header.findIndex(h => h.includes('v2themes'));
                            if (th >= 0) config.columnIndices.themes = th;
                            const pe = header.findIndex(h => h.includes('v2persons'));
                            if (pe >= 0) config.columnIndices.persons = pe;
                            const or = header.findIndex(h => h.includes('v2organizations'));
                            if (or >= 0) config.columnIndices.orgs = or;
                            const di = header.findIndex(h => h.includes('documentidentifier'));
                            if (di >= 0) config.columnIndices.documentIdentifier = di;
                            logger.info(`Header detected, updated indices -> themes:${config.columnIndices.themes}, persons:${config.columnIndices.persons}, orgs:${config.columnIndices.orgs}, docId:${config.columnIndices.documentIdentifier}`);
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
                    const documentIdentifier = getCol(config.columnIndices.documentIdentifier);
                    if (documentIdentifier) collector.documentIdentifiers.push(documentIdentifier);
                } catch (e) {
                    logger.warn(`Row parse error on row ${rowCount}: ${e.message}`);
                }
            })
            .on('end', () => {
                logger.info(`CSV parse completed. Rows: ${rowCount}. Collected themes: ${collector.themes.length}, persons: ${collector.persons.length}, orgs: ${collector.orgs.length}, documentIdentifiers: ${collector.documentIdentifiers.length}`);
                resolve(collector);
            });
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
        logger.info(`Parsed 15-min file ${fifteenName}. Themes: ${collector.themes.length}, Persons: ${collector.persons.length}, Orgs: ${collector.orgs.length}, Docs: ${collector.documentIdentifiers.length}`);
        const ranked = rankCollector(collector);
        logger.info(`Ranked -> T:${ranked.themes.length} P:${ranked.persons.length} O:${ranked.orgs.length}`);
        const dailyDateStr = d.toISOString().slice(0, 10);
        await saveTrends({
            date: dailyDateStr,
            timestamp: dailyDate,
            jobType: 'daily',
            ...ranked,
            documentIdentifiers: collector.documentIdentifiers
        });
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
            logger.info(`Parsed daily file ${dailyName}. Themes: ${collector.themes.length}, Persons: ${collector.persons.length}, Orgs: ${collector.orgs.length}, Docs: ${collector.documentIdentifiers.length}`);
            const ranked = rankCollector(collector);
            logger.info(`Ranked -> T:${ranked.themes.length} P:${ranked.persons.length} O:${ranked.orgs.length}`);
            const dailyDate = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
            await saveTrends({ date: dailyDate, timestamp: dailyDate, jobType: 'daily', ...ranked, documentIdentifiers: collector.documentIdentifiers });
            return true;
        } catch (err) {
            const status = err.response && err.response.status;
            logger.warn(`Daily fetch failed for ${dailyName} (${status || 'no status'}): ${err.message}`);
        }
    }

    logger.error('All fetch attempts failed (15-min and daily fallbacks).');
    return false;
}

async function saveTrends({ date, timestamp, jobType, themes, persons, orgs, documentIdentifiers = [] }) {
    const isoDate = new Date(date).toISOString().slice(0, 10);
    const trends = [
        { type: jobType || 'realtime', date: isoDate, category: 'themes', keywords: themes, timestamp },
        { type: jobType || 'realtime', date: isoDate, category: 'persons', keywords: persons, timestamp },
        { type: jobType || 'realtime', date: isoDate, category: 'orgs', keywords: orgs, timestamp },
    ];

    // Create and add a trend document for document identifiers
    if (documentIdentifiers.length > 0) {
        trends.push({
            type: jobType || 'realtime',
            date: isoDate,
            category: 'documents',
            keywords: documentIdentifiers.map(id => ({ word: id, count: 1 })),
            timestamp
        });
    }

    for (const trendData of trends) {
        if (trendData.keywords && trendData.keywords.length > 0) {
            await Trend.findOneAndUpdate(
                { type: trendData.type, date: trendData.date, category: trendData.category },
                { $set: { keywords: trendData.keywords, timestamp: trendData.timestamp } },
                { upsert: true, new: true }
            );

            logger.info(`Saved/Updated ${trendData.type} ${trendData.category} trends for ${trendData.date}`);

            // DEBUG: Fetch back immediately and log count to verify save
            const savedDoc = await Trend.findOne({ type: trendData.type, date: trendData.date, category: trendData.category }).lean();
            if (savedDoc) {
                logger.debug(`Verified saved ${trendData.category} keywords count: ${savedDoc.keywords?.length || 0}`);
            } else {
                logger.warn(`Failed to verify saved ${trendData.category} trends for ${trendData.date}`);
            }
        } else {
            logger.info(`No keywords for ${trendData.type} ${trendData.category} on ${trendData.date}`);
        }
    }
}


module.exports = { fetchAndProcess, getFilenameForUTC, getDailyFilenameForUTC };