// src/utils/cleaner.js
// simple normalization and tokenization
const STOPWORDS = new Set([
    'the', 'a', 'an', 'and', 'or', 'of', 'in', 'for', 'on', 'with', 'to', 'from', 'by', 'at', 'is', 'was', 'are'
    // add more as needed
]);

function cleanKeyword(raw) {
    if (!raw) return null;
    let s = raw.toString().toLowerCase().trim();
    // remove enclosing punctuation
    s = s.replace(/^[^\w]+|[^\w]+$/g, '');
    // collapse multiple spaces
    s = s.replace(/\s+/g, ' ');
    if (s.length === 0) return null;
    if (STOPWORDS.has(s)) return null;
    return s;
}

// For V2 fields which are semi-colon delimited
function splitAndClean(fieldValue) {
    if (!fieldValue) return [];
    // GKG often uses ';' as delimiter
    const parts = fieldValue.split(';');
    return parts.map(cleanKeyword).filter(Boolean);
}

module.exports = { cleanKeyword, splitAndClean };
