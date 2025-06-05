const express = require('express');
const Ingestion = require("../Models/Ingestion");

const router = express.Router();

router.get('/:ingestion_id', async (req, res) => {
  try {
    const { ingestion_id } = req.params;

    const ingestion = await Ingestion.findOne({ ingestion_id });

    if (!ingestion) {
      return res.status(404).json({ error: 'Ingestion not found' });
    }

    
    let overallStatus = 'yet_to_start';
    const batchStatuses = ingestion.batches.map(b => b.status);
    
    if (batchStatuses.every(status => status === 'completed')) {
      overallStatus = 'completed';
    } else if (batchStatuses.some(status => status === 'triggered' || status === 'completed')) {
      overallStatus = 'triggered';
    }

    
    if (overallStatus !== ingestion.status) {
      ingestion.status = overallStatus;
      await ingestion.save();
    }

    const response = {
      ingestion_id,
      status: overallStatus,
      batches: ingestion.batches.map(batch => ({
        batch_id: batch.batch_id,
        ids: batch.ids,
        status: batch.status
      }))
    };

    res.json(response);
  } catch (error) {
    console.error('Error in status endpoint:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;