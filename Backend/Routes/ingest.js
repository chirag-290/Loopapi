const express = require('express');
const { v4: uuidv4 } = require('uuid');
const Ingestion = require("../Models/Ingestion");
const { addToQueue } = require("../Servcies/QueueManager");

const router = express.Router();

router.post('/', async (req, res) => {
  try {
    const { ids, priority } = req.body;

   
    if (!ids || !Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ error: 'ids must be a non-empty array' });
    }

    if (!priority || !['HIGH', 'MEDIUM', 'LOW'].includes(priority)) {
      return res.status(400).json({ error: 'priority must be HIGH, MEDIUM, or LOW' });
    }

    
    const maxId = Math.pow(10, 9) + 7;
    for (const id of ids) {
      if (!Number.isInteger(id) || id < 1 || id > maxId) {
        return res.status(400).json({ 
          error: `All ids must be integers between 1 and ${maxId}` 
        });
      }
    }

    const ingestion_id = uuidv4();

    
    const batches = [];
    for (let i = 0; i < ids.length; i += 3) {
      const batchIds = ids.slice(i, i + 3);
      batches.push({
        batch_id: uuidv4(),
        ids: batchIds,
        status: 'yet_to_start'
      });
    }

   
    const ingestion = new Ingestion({
      ingestion_id,
      priority,
      batches,
      status: 'yet_to_start'
    });

    await ingestion.save();

   
    for (const batch of batches) {
      await addToQueue({
        ingestion_id,
        batch_id: batch.batch_id,
        ids: batch.ids,
        priority,
        created_at: ingestion.created_at
      });
    }

    res.json({ ingestion_id });
  } catch (error) {
    console.error('Error in ingest endpoint:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;