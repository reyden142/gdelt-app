// src/index.js
const express = require('express');
const mongoose = require('mongoose');
const routes = require('./routes/trends');
const config = require('./config');
const { startSchedules } = require('./scheduler');
const winston = require('winston');

const logger = winston.createLogger({ transports: [new winston.transports.Console()] });

async function start() {
    // connect to Mongo
    await mongoose.connect(config.mongoUri, { useNewUrlParser: true, useUnifiedTopology: true });
    logger.info('Connected to MongoDB');

    const app = express();
    app.use(express.json());
    app.use('/trends', routes);

    const port = config.port || 3000;
    app.listen(port, () => {
        logger.info(`Server listening on port ${port}`);
    });

    // start scheduled jobs
    startSchedules();
}

start().catch(err => {
    console.error(err);
    process.exit(1);
});
