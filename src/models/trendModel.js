// src/models/trendModel.js
const mongoose = require('mongoose');

const KeywordSchema = new mongoose.Schema({
    word: { type: String, required: true },
    count: { type: Number, required: true },
    score: { type: Number, required: false }
}, { _id: false });

const TrendSchema = new mongoose.Schema({
    timestamp: { type: Date, required: true, index: true },
    type: { type: String, enum: ['realtime', 'daily', 'ranked'], required: true, index: true },
    date: { type: String, required: true, index: true }, // YYYY-MM-DD (useful)
    category: { type: String, enum: ['themes', 'persons', 'orgs', 'all'], default: 'all', index: true },
    keywords: [KeywordSchema]
}, { timestamps: true });

TrendSchema.index({ type: 1, date: 1, category: 1 });

module.exports = mongoose.model('Trend', TrendSchema);
