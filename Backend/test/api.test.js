const request = require('supertest');
const mongoose = require('mongoose');
const app = require('../server');
const Ingestion = require("../Models/Ingestion");

// Test database
const MONGODB_URI = process.env.MONGODB_TEST_URI || 'mongodb://localhost:27017/data-ingestion-test';

beforeAll(async () => {
  await mongoose.connect(MONGODB_URI, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });
});

beforeEach(async () => {
  await Ingestion.deleteMany({});
});

afterAll(async () => {
  await mongoose.connection.close();
});

describe('Data Ingestion API Tests', () => {
  
  describe('POST /ingest', () => {
    test('should accept valid ingestion request', async () => {
      const response = await request(app)
        .post('/ingest')
        .send({
          ids: [1, 2, 3, 4, 5],
          priority: 'HIGH'
        });

      expect(response.status).toBe(200);
      expect(response.body).toHaveProperty('ingestion_id');
      expect(typeof response.body.ingestion_id).toBe('string');
    });

    test('should reject invalid priority', async () => {
      const response = await request(app)
        .post('/ingest')
        .send({
          ids: [1, 2, 3],
          priority: 'INVALID'
        });

      expect(response.status).toBe(400);
      expect(response.body).toHaveProperty('error');
    });

    test('should reject empty ids array', async () => {
      const response = await request(app)
        .post('/ingest')
        .send({
          ids: [],
          priority: 'HIGH'
        });

      expect(response.status).toBe(400);
      expect(response.body).toHaveProperty('error');
    });

    test('should reject ids outside valid range', async () => {
      const maxId = Math.pow(10, 9) + 8; 
      const response = await request(app)
        .post('/ingest')
        .send({
          ids: [1, 2, maxId],
          priority: 'HIGH'
        });

      expect(response.status).toBe(400);
      expect(response.body).toHaveProperty('error');
    });

    test('should create correct number of batches', async () => {
      const response = await request(app)
        .post('/ingest')
        .send({
          ids: [1, 2, 3, 4, 5, 6, 7],
          priority: 'MEDIUM'
        });

      expect(response.status).toBe(200);

      const ingestion = await Ingestion.findOne({ 
        ingestion_id: response.body.ingestion_id 
      });

      expect(ingestion.batches).toHaveLength(3); 
      expect(ingestion.batches[0].ids).toHaveLength(3);
      expect(ingestion.batches[1].ids).toHaveLength(3);
      expect(ingestion.batches[2].ids).toHaveLength(1);
    });
  });

  describe('GET /status/:ingestion_id', () => {
    test('should return status for valid ingestion_id', async () => {
      
      const ingestResponse = await request(app)
        .post('/ingest')
        .send({
          ids: [1, 2, 3],
          priority: 'HIGH'
        });

      const ingestion_id = ingestResponse.body.ingestion_id;

      
      const statusResponse = await request(app)
        .get(`/status/${ingestion_id}`);

      expect(statusResponse.status).toBe(200);
      expect(statusResponse.body).toHaveProperty('ingestion_id', ingestion_id);
      expect(statusResponse.body).toHaveProperty('status');
      expect(statusResponse.body).toHaveProperty('batches');
      expect(Array.isArray(statusResponse.body.batches)).toBe(true);
    });

    test('should return 404 for invalid ingestion_id', async () => {
      const response = await request(app)
        .get('/status/invalid-id');

      expect(response.status).toBe(404);
      expect(response.body).toHaveProperty('error');
    });

    test('should show correct batch structure', async () => {
      const ingestResponse = await request(app)
        .post('/ingest')
        .send({
          ids: [1, 2, 3, 4],
          priority: 'LOW'
        });

      const statusResponse = await request(app)
        .get(`/status/${ingestResponse.body.ingestion_id}`);

      expect(statusResponse.body.batches).toHaveLength(2);
      
      const batch = statusResponse.body.batches[0];
      expect(batch).toHaveProperty('batch_id');
      expect(batch).toHaveProperty('ids');
      expect(batch).toHaveProperty('status');
      expect(['yet_to_start', 'triggered', 'completed']).toContain(batch.status);
    });
  });

  describe('Priority and Rate Limiting Tests', () => {
    test('should process HIGH priority before MEDIUM priority', async () => {
      
      const mediumResponse = await request(app)
        .post('/ingest')
        .send({
          ids: [1, 2, 3],
          priority: 'MEDIUM'
        });

      
      await new Promise(resolve => setTimeout(resolve, 1000));

      const highResponse = await request(app)
        .post('/ingest')
        .send({
          ids: [4, 5, 6],
          priority: 'HIGH'
        });

    
      await new Promise(resolve => setTimeout(resolve, 8000));

      const highStatus = await request(app)
        .get(`/status/${highResponse.body.ingestion_id}`);
      
      const mediumStatus = await request(app)
        .get(`/status/${mediumResponse.body.ingestion_id}`);

      expect(highStatus.status).toBe(200);
      expect(mediumStatus.status).toBe(200);
    }, 15000);

    test('should respect rate limit of 1 batch per 5 seconds', async () => {
      const startTime = Date.now();

    
      const response1 = await request(app)
        .post('/ingest')
        .send({
          ids: [1, 2, 3],
          priority: 'HIGH'
        });

      const response2 = await request(app)
        .post('/ingest')
        .send({
          ids: [4, 5, 6],
          priority: 'HIGH'
        });

   
      await new Promise(resolve => setTimeout(resolve, 12000));

      const status1 = await request(app)
        .get(`/status/${response1.body.ingestion_id}`);
      
      const status2 = await request(app)
        .get(`/status/${response2.body.ingestion_id}`);

     
      const hasProcessing = 
        status1.body.status !== 'yet_to_start' || 
        status2.body.status !== 'yet_to_start';

      expect(hasProcessing).toBe(true);
    }, 20000);
  });

  describe('Edge Cases', () => {
    test('should handle large number of IDs', async () => {
      const largeIdArray = Array.from({ length: 100 }, (_, i) => i + 1);
      
      const response = await request(app)
        .post('/ingest')
        .send({
          ids: largeIdArray,
          priority: 'MEDIUM'
        });

      expect(response.status).toBe(200);

      const statusResponse = await request(app)
        .get(`/status/${response.body.ingestion_id}`);

      expect(statusResponse.body.batches).toHaveLength(34); 
    });

    test('should handle single ID', async () => {
      const response = await request(app)
        .post('/ingest')
        .send({
          ids: [42],
          priority: 'LOW'
        });

      expect(response.status).toBe(200);

      const statusResponse = await request(app)
        .get(`/status/${response.body.ingestion_id}`);

      expect(statusResponse.body.batches).toHaveLength(1);
      expect(statusResponse.body.batches[0].ids).toEqual([42]);
    });

    test('should handle maximum valid ID', async () => {
      const maxId = Math.pow(10, 9) + 7;
      
      const response = await request(app)
        .post('/ingest')
        .send({
          ids: [maxId],
          priority: 'HIGH'
        });

      expect(response.status).toBe(200);
    });
  });
});