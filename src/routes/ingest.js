const express = require('express');
const router = express.Router();
const ingestionService = require('../services/ingestionService');

router.post('/', (req, res) => {
  try {
    const { ids, priority } = req.body;

    // Validate input
    if (!Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ error: 'ids must be a non-empty array' });
    }

    if (!['HIGH', 'MEDIUM', 'LOW'].includes(priority)) {
      return res.status(400).json({ error: 'priority must be HIGH, MEDIUM, or LOW' });
    }

    // Validate ID range
    if (ids.some(id => !Number.isInteger(id) || id < 1 || id > 1e9 + 7)) {
      return res.status(400).json({ error: 'ids must be integers between 1 and 10^9+7' });
    }

    const ingestionId = ingestionService.createIngestion(ids, priority);
    res.json({ ingestion_id: ingestionId });
  } catch (error) {
    console.error('Error in ingestion:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router; 