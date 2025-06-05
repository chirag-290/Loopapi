const mongoose = require('mongoose');

const queueItemSchema = new mongoose.Schema({
  ingestion_id: String,
  batch_id: String,
  ids: [Number],
  priority: {
    type: String,
    enum: ['HIGH', 'MEDIUM', 'LOW']
  },
  created_at: Date,
  status: {
    type: String,
    enum: ['pending', 'processing', 'completed'],
    default: 'pending'
  }
});

const QueueItem = mongoose.model('QueueItem', queueItemSchema);

const priorityOrder = { HIGH: 3, MEDIUM: 2, LOW: 1 };

async function addToQueue(item) {
  const queueItem = new QueueItem(item);
  await queueItem.save();
  console.log(`Added batch ${item.batch_id} to queue with priority ${item.priority}`);
}

async function getNextBatch() {
  
  const nextItem = await QueueItem.findOne({ status: 'pending' })
    .sort({ 
      priority: -1, 
      created_at: 1 
    });

  if (!nextItem) return null;


  const allPending = await QueueItem.find({ status: 'pending' });
  
  allPending.sort((a, b) => {
    const priorityDiff = priorityOrder[b.priority] - priorityOrder[a.priority];
    if (priorityDiff !== 0) return priorityDiff;
    return new Date(a.created_at) - new Date(b.created_at);
  });

  if (allPending.length === 0) return null;

  const selectedItem = allPending[0];
  selectedItem.status = 'processing';
  await selectedItem.save();

  return selectedItem;
}

async function markBatchCompleted(batch_id) {
  await QueueItem.findOneAndUpdate(
    { batch_id },
    { status: 'completed' }
  );
}

module.exports = {
  addToQueue,
  getNextBatch,
  markBatchCompleted
};