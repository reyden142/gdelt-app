// src/services/parser.js
const csv = require('fast-csv');
const config = require('../config');
const { splitAndClean } = require('../utils/cleaner');
const aggregator = require('./aggregator');
const winston = require('winston');

const logger = winston.createLogger({ transports: [new winston.transports.Console()] });

/**
 * parseCsvStream - accepts a readable stream of the CSV file content (unzipped)
 * opts:
 *   - category: 'all' | 'themes' | 'persons' | 'orgs'
 *   - timestamp: Date (JS Date object) - the time this file represents
 */
async function parseCsvStream(stream, opts = {}) {
    const category = opts.category || 'all';
    const timestamp = opts.timestamp || new Date();
    const themesIdx = config.columnIndices.themes;
    const personsIdx = config.columnIndices.persons;
    const orgsIdx = config.columnIndices.orgs;
    const locationsIdx = config.columnIndices.locations;
    // We'll attempt to detect header on first row
    let headerDetected = false;
    let header = null;
    let rowCount = 0;

    // create collectors (to be aggregated for this file)
    const collector = {
        themes: [],
        persons: [],
        orgs: [],
    };

    return new Promise((resolve, reject) => {
        const parserStream = csv.parse({ headers: false, relax_quotes: true, skipLines: 0 })
            .on('error', error => {
                logger.error('CSV parse error: ' + error.message);
                reject(error);
            })
            .on('data', row => {
                rowCount++;
                try {
                    // row is array-like (fast-csv returns array when headers:false)
                    // On first row, try to detect header by checking for V2Themes etc
                    if (!headerDetected && rowCount === 1) {
                        const rowStr = row.join('|').toLowerCase();
                        if (rowStr.includes('v2themes') || rowStr.includes('v2persons') || rowStr.includes('v2organizations')) {
                            headerDetected = true;
                            header = row.map(c => String(c).toLowerCase());
                            // map indices
                            if (!config.columnIndices.themes) {
                                const i = header.findIndex(h => h.includes('v2themes'));
                                if (i >= 0) config.columnIndices.themes = i;
                            }
                            if (!config.columnIndices.persons) {
                                const i = header.findIndex(h => h.includes('v2persons'));
                                if (i >= 0) config.columnIndices.persons = i;
                            }
                            if (!config.columnIndices.orgs) {
                                const i = header.findIndex(h => h.includes('v2organizations'));
                                if (i >= 0) config.columnIndices.orgs = i;
                            }
                            // header row -> skip processing as data
                            return;
                        } else {
                            headerDetected = false;
                        }
                    }

                    // helper to safely get col
                    const getCol = (idx) => {
                        if (idx === null || idx === undefined) return null;
                        return row[idx] !== undefined ? row[idx] : null;
                    };

                    if (category === 'all' || category === 'themes') {
                        const rawThemes = getCol(config.columnIndices.themes);
                        if (rawThemes) {
                            const parts = splitAndClean(rawThemes);
                            collector.themes.push(...parts);
                        }
                    }
                    if (category === 'all' || category === 'persons') {
                        const rawPersons = getCol(config.columnIndices.persons);
                        if (rawPersons) {
                            const parts = splitAndClean(rawPersons);
                            collector.persons.push(...parts);
                        }
                    }
                    if (category === 'all' || category === 'orgs') {
                        const rawOrgs = getCol(config.columnIndices.orgs);
                        if (rawOrgs) {
                            const parts = splitAndClean(rawOrgs);
                            collector.orgs.push(...parts);
                        }
                    }
                } catch (err) {
                    // non-fatal per-row errors should be logged and not stop the stream
                    logger.warn('Row parse warning: ' + err.message);
                }
            })
            .on('end', async (rowCount) => {
                logger.info(`CSV parse completed — rows: ${rowCount}`);
                // pass collected tokens to aggregator which will rank and persist
                try {
                    await aggregator.aggregateFromFile({ collector, timestamp, category });
                    resolve(true);
                } catch (err) {
                    reject(err);
                }
            });

        stream.pipe(parserStream);
    });
}

module.exports = { parseCsvStream };
