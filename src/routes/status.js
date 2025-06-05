const express = require('express');
const router = express.Router();
const ingestionService = require('../services/ingestionService');

router.get('/:ingestionId', (req, res) => {
  try {
    const { ingestionId } = req.params;
    const status = ingestionService.getIngestionStatus(ingestionId);

    if (!status) {
      return res.status(404).json({ error: 'Ingestion not found' });
    }

    res.json(status);
  } catch (error) {
    console.error('Error getting status:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router; 