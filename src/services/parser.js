// src/services/parser.js
const csv = require('fast-csv');
const config = require('../config');
const { splitAndClean } = require('../utils/cleaner');
const aggregator = require('./aggregator');
const winston = require('winston');

const logger = winston.createLogger({ transports: [new winston.transports.Console()] });

async function parseCsvStream(stream, opts = {}) {
    const category = opts.category || 'all';
    const timestamp = opts.timestamp || new Date();
    let headerDetected = false;
    let header = null;
    let rowCount = 0;

    // create collectors (to be aggregated for this file)
    const collector = {
        themes: [],
        persons: [],
        orgs: [],
        documentIdentifiers: [],
    };

    return new Promise((resolve, reject) => {
        const parserStream = csv.parse({ headers: false, relax_quotes: true, skipLines: 0, delimiter: '\t' })
            .on('error', error => {
                logger.error('CSV parse error: ' + error.message);
                reject(error);
            })
            .on('data', row => {
                rowCount++;
                try {
                    if (!headerDetected && rowCount === 1) {
                        const rowStr = row.join('|').toLowerCase();
                        if (rowStr.includes('v2themes') || rowStr.includes('v2persons') || rowStr.includes('v2organizations') || rowStr.includes('documentidentifier')) {
                            headerDetected = true;
                            header = row.map(c => String(c).toLowerCase());
                            const themesIdx = header.findIndex(h => h.includes('v2themes'));
                            if (themesIdx >= 0) config.columnIndices.themes = themesIdx;
                            const personsIdx = header.findIndex(h => h.includes('v2persons'));
                            if (personsIdx >= 0) config.columnIndices.persons = personsIdx;
                            const orgsIdx = header.findIndex(h => h.includes('v2organizations'));
                            if (orgsIdx >= 0) config.columnIndices.orgs = orgsIdx;
                            const docIdIdx = header.findIndex(h => h.includes('documentidentifier'));
                            if (docIdIdx >= 0) config.columnIndices.documentIdentifier = docIdIdx;
                            return; // skip header row
                        }
                    }

                    const getCol = (idx) => {
                        if (idx === null || idx === undefined) return null;
                        return row[idx] !== undefined ? row[idx] : null;
                    };

                    // Get document IDs for this row, split by '|' if multiple
                    const rawDocId = getCol(config.columnIndices.documentIdentifier);
                    const docIds = rawDocId ? rawDocId.split('|').filter(id => id.trim() !== '') : [];

                    // Helper to build keyword objects with documents attached
                    const buildKeywordObjs = (raw) => {
                        return splitAndClean(raw).map(word => ({
                            word,
                            count: 1,
                            documents: docIds,
                        }));
                    };

                    if (category === 'all' || category === 'themes') {
                        const rawThemes = getCol(config.columnIndices.themes);
                        if (rawThemes) {
                            collector.themes.push(...buildKeywordObjs(rawThemes));
                        }
                    }
                    if (category === 'all' || category === 'persons') {
                        const rawPersons = getCol(config.columnIndices.persons);
                        if (rawPersons) {
                            collector.persons.push(...buildKeywordObjs(rawPersons));
                        }
                    }
                    if (category === 'all' || category === 'orgs') {
                        const rawOrgs = getCol(config.columnIndices.orgs);
                        if (rawOrgs) {
                            collector.orgs.push(...buildKeywordObjs(rawOrgs));
                        }
                    }

                    // Collect document identifiers separately too (raw string)
                    if (rawDocId) {
                        collector.documentIdentifiers.push(...docIds);
                    }

                } catch (err) {
                    logger.warn('Row parse warning: ' + err.message);
                }
            })
            .on('end', async (rowCount) => {
                logger.info(`CSV parse completed — rows: ${rowCount}`);
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
