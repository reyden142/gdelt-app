// src/scheduler.js
const cron = require('node-cron');
const config = require('./config');
const { fetchAndProcess, getFilenameForUTC } = require('./services/gdeltFetcher');
const aggregator = require('./services/aggregator');
const parser = require('./services/parser');
const axios = require('axios');
const unzipper = require('unzipper');
const winston = require('winston');
const csv = require('fast-csv'); // Re-adding fast-csv

const logger = winston.createLogger({ transports: [new winston.transports.Console()] });

function startSchedules() {
    // Real-time schedule: every N minutes (*/N * * * *)
    const interval = config.realtimeIntervalMin || 15;
    const cronExpr = `*/${interval} * * * *`;
    cron.schedule(cronExpr, async () => {
        try {
            const now = new Date(); // local time – we will treat as UTC when forming filename
            // align minutes to nearest multiple of interval downward (match GDELT file timestamps)
            const m = Math.floor(now.getUTCMinutes() / interval) * interval;
            const dateForFile = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), now.getUTCHours(), m, 0));
            await fetchAndProcess(dateForFile, { category: 'all', timestamp: dateForFile, type: 'realtime' }); // Re-adding 'type'
        } catch (err) {
            logger.error('Real-time job error: ' + err.message);
        }
    }, { timezone: 'UTC' });

    // Daily job: at dailyHourUTC:00 UTC
    const dailyHour = config.dailyHourUTC || 0;
    const dailyCron = `0 ${dailyHour} * * *`;
    cron.schedule(dailyCron, async () => {
        try {
            // build list of the last 24 hours worth of file timestamps (every 15 mins => 96 files)
            const intervalMin = config.realtimeIntervalMin || 15;
            const filesToFetch = 96; // default 24h at 15-min
            const now = new Date();
            const collectors = [];
            for (let i = 0; i < filesToFetch; i++) {
                const minutesBack = i * intervalMin;
                const date = new Date(now.getTime() - minutesBack * 60_000);
                // align timestamp
                const m = Math.floor(date.getUTCMinutes() / intervalMin) * intervalMin;
                const fileDate = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), date.getUTCHours(), m, 0));
                const filename = getFilenameForUTC(fileDate);
                const url = `${config.gdeltBaseUrl}/${filename}`;
                try {
                    const resp = await axios({ url, method: 'GET', responseType: 'stream', timeout: 60_000 });
                    const unzipStream = resp.data.pipe(unzipper.ParseOne());
                    // parse stream to collector
                    // parser.parseCsvStream will internally call aggregator.aggregateFromFile, but for daily we want to collect results
                    // so we will collect tokens per file
                    // We'll hack: adjust parser to accumulate into temporary collector by intercepting output; simplest: parse into an in-memory collector using a local version
                    const localCollector = { themes: [], persons: [], orgs: [] };
                    await new Promise((resolve, reject) => {
                        unzipStream.pipe(csv.parse({ headers: false, relax_quotes: true }))
                            .on('data', row => {
                                if (!config.columnIndices.themes) {
                                    const rowStr = row.join('|').toLowerCase();
                                    if (rowStr.includes('v2themes') || rowStr.includes('v2persons') || rowStr.includes('v2organizations')) {
                                        // header present - update config indices if not set (parser does this too)
                                        const header = row.map(c => String(c).toLowerCase());
                                        const i = header.findIndex(h => h.includes('v2themes'));
                                        if (i >= 0) config.columnIndices.themes = i;
                                        const i2 = header.findIndex(h => h.includes('v2persons'));
                                        if (i2 >= 0) config.columnIndices.persons = i2;
                                        const i3 = header.findIndex(h => h.includes('v2organizations'));
                                        if (i3 >= 0) config.columnIndices.orgs = i3;
                                        return; // skip header
                                    }
                                }
                                // safe get
                                const getCol = (idx) => (idx !== null && row[idx] !== undefined) ? row[idx] : null;
                                if (config.columnIndices.themes) {
                                    const raw = getCol(config.columnIndices.themes);
                                    if (raw) localCollector.themes.push(...require('./utils/cleaner').splitAndClean(raw));
                                }
                                if (config.columnIndices.persons) {
                                    const raw = getCol(config.columnIndices.persons);
                                    if (raw) localCollector.persons.push(...require('./utils/cleaner').splitAndClean(raw));
                                }
                                if (config.columnIndices.orgs) {
                                    const raw = getCol(config.columnIndices.orgs);
                                    if (raw) localCollector.orgs.push(...require('./utils/cleaner').splitAndClean(raw));
                                }
                            })
                            .on('end', () => resolve(true))
                            .on('error', (err) => reject(err));
                    });
                    collectors.push(localCollector);
                } catch (err) {
                    // skip missing file quietly
                    logger.warn(`Daily fetch skip ${url} - ${err.message}`);
                }
            } // end for
            // now aggregate daily
            const dateStr = new Date().toISOString().slice(0, 10);
            await aggregator.aggregateDaily({ collectorsArray: collectors, date: dateStr, category: 'all' });
        } catch (err) {
            logger.error('Daily job error: ' + err.message);
        }
    }, { timezone: 'UTC' });

    logger.info('Schedulers started');
}

module.exports = { startSchedules };
