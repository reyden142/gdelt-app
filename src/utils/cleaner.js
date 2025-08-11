// src/utils/cleaner.js
// simple normalization and tokenization
const STOPWORDS = new Set([
    'the', 'a', 'an', 'and', 'or', 'of', 'in', 'for', 'on', 'with', 'to', 'from', 'by', 'at', 'is', 'was', 'are'
]);

function cleanKeyword(raw) {
    if (!raw) return null;
    let s = raw.toString().toLowerCase().trim();
    s = s.replace(/^[^\w]+|[^\w]+$/g, '');
    s = s.replace(/\s+/g, ' ');
    if (s.length === 0) return null;
    if (STOPWORDS.has(s)) return null;
    return s;
}

function splitAndClean(fieldValue) {
    if (!fieldValue) return [];
    const parts = fieldValue.split(';');
    // Clean and also drop tokens considered noise (domains, urls, numeric vectors, mostly digits/punct)
    return parts
        .map(cleanKeyword)
        .filter(Boolean)
        .filter(token => !isNoiseToken(token));
}

function isStrictDomain(token) {
    const s = String(token).toLowerCase();
    // match full domain like example.com or sub.example.co.uk
    return /^[a-z0-9.-]+\.[a-z]{2,}$/.test(s) && !s.includes(' ');
}

function isUrl(token) {
    const s = String(token).toLowerCase();
    return /^https?:\/\//.test(s) || /^www\./.test(s);
}

function isNumericVector(token) {
    if (!token) return false;
    return /^\d+(?:\.\d+)?(?:,\d+(?:\.\d+)?){3,}$/.test(String(token));
}

function isMostlyDigitsOrPunct(token) {
    if (!token) return false;
    const s = String(token);
    const digits = (s.match(/[0-9]/g) || []).length;
    return digits / Math.max(s.length, 1) > 0.6;
}

function isNoiseToken(token) {
    if (!token) return true;
    const s = String(token).trim();
    if (s.length < 3) return true;
    if (isUrl(s) || isStrictDomain(s)) return true;
    if (isNumericVector(s)) return true;
    if (isMostlyDigitsOrPunct(s)) return true;
    return false;
}

function filterNoiseKeywords(keywords) {
    if (!Array.isArray(keywords)) return [];
    return keywords.filter(k => k && k.word && !isNoiseToken(k.word));
}

module.exports = { cleanKeyword, splitAndClean, isNoiseToken, filterNoiseKeywords, isNumericVector };
