const Ingestion = require("../Models/Ingestion");
const { getNextBatch, markBatchCompleted } = require("./QueueManager");

let isProcessing = false;
let lastProcessTime = 0;
const RATE_LIMIT_MS = 5000; 

async function simulateExternalAPI(id) {
 
  const delay = Math.random() * 1000 + 500; 
  await new Promise(resolve => setTimeout(resolve, delay));
  return { id, data: "processed" };
}

async function processBatch(batchItem) {
  console.log(`Processing batch ${batchItem.batch_id} with IDs: ${batchItem.ids}`);

  
  await Ingestion.findOneAndUpdate(
    { 
      ingestion_id: batchItem.ingestion_id,
      'batches.batch_id': batchItem.batch_id 
    },
    { 
      $set: { 
        'batches.$.status': 'triggered',
        'batches.$.started_at': new Date()
      } 
    }
  );

 
  const results = [];
  for (const id of batchItem.ids) {
    try {
      const result = await simulateExternalAPI(id);
      results.push(result);
      console.log(`Processed ID ${id}`);
    } catch (error) {
      console.error(`Error processing ID ${id}:`, error);
    }
  }

 
  await Ingestion.findOneAndUpdate(
    { 
      ingestion_id: batchItem.ingestion_id,
      'batches.batch_id': batchItem.batch_id 
    },
    { 
      $set: { 
        'batches.$.status': 'completed',
        'batches.$.completed_at': new Date()
      } 
    }
  );

  await markBatchCompleted(batchItem.batch_id);
  console.log(`Completed batch ${batchItem.batch_id}`);
}

async function processQueue() {
  if (isProcessing) return;

  const now = Date.now();
  if (now - lastProcessTime < RATE_LIMIT_MS) {
    return;
  }

  isProcessing = true;

  try {
    const nextBatch = await getNextBatch();
    if (nextBatch) {
      lastProcessTime = now;
      await processBatch(nextBatch);
    }
  } catch (error) {
    console.error('Error processing batch:', error);
  } finally {
    isProcessing = false;
  }
}

function startBatchProcessor() {
  console.log('Starting batch processor...');
  
  
  setInterval(processQueue, 1000);
}

module.exports = {
  startBatchProcessor,
  processBatch
};