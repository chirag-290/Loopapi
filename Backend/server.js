
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
require('dotenv').config();

const ingestRoutes = require("./Routes/ingest");
const statusRoutes = require("./Routes/status");
const { startBatchProcessor } = require("./Servcies/BatchProcessor");

const app = express();
const PORT = process.env.PORT || 5000;


app.use(cors());
app.use(express.json());


mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/data-ingestion', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

mongoose.connection.on('connected', () => {
  console.log('Connected to MongoDB');
});

mongoose.connection.on('error', (err) => {
  console.error('MongoDB connection error:', err);
});


app.use('/ingest', ingestRoutes);
app.use('/status', statusRoutes);


app.get('/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});


startBatchProcessor();

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

module.exports = app;