// src/utils/ranker.js
function rankByCount(arr, topN = 50) {
    // arr: array of keywords (strings)
    const counts = Object.create(null);
    arr.forEach(k => {
        counts[k] = (counts[k] || 0) + 1;
    });
    const result = Object.entries(counts)
        .map(([word, count]) => ({ word, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, topN);
    return result;
}

module.exports = { rankByCount };
