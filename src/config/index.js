// src/config/index.js
require('dotenv').config();

module.exports = {
    mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27017/gdelt_trends',
    redis: {
        host: process.env.REDIS_HOST || '127.0.0.1',
        port: Number(process.env.REDIS_PORT) ? Number(process.env.REDIS_PORT) : 6379,
        password: process.env.REDIS_PASSWORD || undefined,
    },
    gdeltBaseUrl: process.env.GDELT_BASE_URL || 'http://data.gdeltproject.org/gkg',
    gdeltDailyBaseUrl: process.env.GDELT_DAILY_BASE_URL || 'http://data.gdeltproject.org/gkg',
    realtimeIntervalMin: Number(process.env.REALTIME_INTERVAL_MIN || 15),
    dailyHourUTC: Number(process.env.DAILY_HOUR_UTC || 0),
    columnIndices: {
        themes: process.env.V2THEMES_INDEX !== '' && process.env.V2THEMES_INDEX !== undefined ? Number(process.env.V2THEMES_INDEX) : null,
        persons: process.env.V2PERSONS_INDEX !== '' && process.env.V2PERSONS_INDEX !== undefined ? Number(process.env.V2PERSONS_INDEX) : null,
        orgs: process.env.V2ORGS_INDEX !== '' && process.env.V2ORGS_INDEX !== undefined ? Number(process.env.V2ORGS_INDEX) : null,
        locations: process.env.V2LOCATIONS_INDEX !== '' && process.env.V2LOCATIONS_INDEX !== undefined ? Number(process.env.V2LOCATIONS_INDEX) : null,
        tone: process.env.V2TONE_INDEX !== '' && process.env.V2TONE_INDEX !== undefined ? Number(process.env.V2TONE_INDEX) : null,
        dateAdded: process.env.DATEADDED_INDEX !== '' && process.env.DATEADDED_INDEX !== undefined ? Number(process.env.DATEADDED_INDEX) : null,
    },
    topN: Number(process.env.TOP_N || 50),
    port: Number(process.env.PORT || 3000)
};
