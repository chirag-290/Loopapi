const mongoose = require('mongoose');

const batchSchema = new mongoose.Schema({
  batch_id: {
    type: String,
    required: true
  },
  ids: [{
    type: Number,
    required: true
  }],
  status: {
    type: String,
    enum: ['yet_to_start', 'triggered', 'completed'],
    default: 'yet_to_start'
  },
  created_at: {
    type: Date,
    default: Date.now
  },
  started_at: Date,
  completed_at: Date
});

const ingestionSchema = new mongoose.Schema({
  ingestion_id: {
    type: String,
    required: true,
    unique: true
  },
  priority: {
    type: String,
    enum: ['HIGH', 'MEDIUM', 'LOW'],
    required: true
  },
  status: {
    type: String,
    enum: ['yet_to_start', 'triggered', 'completed'],
    default: 'yet_to_start'
  },
  batches: [batchSchema],
  created_at: {
    type: Date,
    default: Date.now
  }
});

module.exports = mongoose.model('Ingestion', ingestionSchema);