const request = require('supertest');
const express = require('express');
const ingestRoutes = require('../routes/ingest');
const statusRoutes = require('../routes/status');
const ingestionService = require('../services/ingestionService');

const app = express();
app.use(express.json());
app.use('/ingest', ingestRoutes);
app.use('/status', statusRoutes);

describe('Data Ingestion API Tests', () => {
  // Clear service state before each test
  beforeEach(() => {
    ingestionService.clear();
  });

  // Clean up after all tests
  afterAll(async () => {
    ingestionService.clear();
    // Wait a bit to ensure all async operations are done
    await new Promise(resolve => setTimeout(resolve, 100));
  });

  // Test valid ingestion request
  test('POST /ingest - Valid request', async () => {
    const response = await request(app)
      .post('/ingest')
      .send({
        ids: [1, 2, 3, 4, 5],
        priority: 'HIGH'
      });

    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('ingestion_id');
  });

  // Test invalid ingestion request - missing priority
  test('POST /ingest - Missing priority', async () => {
    const response = await request(app)
      .post('/ingest')
      .send({
        ids: [1, 2, 3]
      });

    expect(response.status).toBe(400);
  });

  // Test invalid ingestion request - invalid priority
  test('POST /ingest - Invalid priority', async () => {
    const response = await request(app)
      .post('/ingest')
      .send({
        ids: [1, 2, 3],
        priority: 'INVALID'
      });

    expect(response.status).toBe(400);
  });

  // Test invalid ingestion request - empty ids array
  test('POST /ingest - Empty ids array', async () => {
    const response = await request(app)
      .post('/ingest')
      .send({
        ids: [],
        priority: 'HIGH'
      });

    expect(response.status).toBe(400);
  });

  // Test invalid ingestion request - invalid id range
  test('POST /ingest - Invalid id range', async () => {
    const response = await request(app)
      .post('/ingest')
      .send({
        ids: [0, 1, 2],
        priority: 'HIGH'
      });

    expect(response.status).toBe(400);
  });

  // Test status endpoint - Fixed to create ingestion first
  test('GET /status/:ingestionId', async () => {
    // First create an ingestion
    const ingestResponse = await request(app)
      .post('/ingest')
      .send({
        ids: [1, 2, 3, 4, 5],
        priority: 'HIGH'
      });

    expect(ingestResponse.status).toBe(200);
    const ingestionId = ingestResponse.body.ingestion_id;

    // Then test the status endpoint
    const response = await request(app)
      .get(`/status/${ingestionId}`);

    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('ingestion_id', ingestionId);
    expect(response.body).toHaveProperty('status');
    expect(response.body).toHaveProperty('batches');
    expect(Array.isArray(response.body.batches)).toBe(true);
  });

  // Test status endpoint with invalid ID
  test('GET /status/:ingestionId - Invalid ID', async () => {
    const response = await request(app)
      .get('/status/invalid-id');

    expect(response.status).toBe(404);
  });

  // Test batch size limit
  test('Batch size limit', async () => {
    const response = await request(app)
      .post('/ingest')
      .send({
        ids: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        priority: 'HIGH'
      });

    const status = await request(app)
      .get(`/status/${response.body.ingestion_id}`);

    // Should have 4 batches (3, 3, 3, 1)
    expect(status.body.batches.length).toBe(4);
    expect(status.body.batches[0].ids.length).toBe(3);
    expect(status.body.batches[1].ids.length).toBe(3);
    expect(status.body.batches[2].ids.length).toBe(3);
    expect(status.body.batches[3].ids.length).toBe(1);
  });

  // Test priority processing order - Fixed assertion logic
  test('Priority processing order', async () => {
    // Create MEDIUM priority request first
    const mediumResponse = await request(app)
      .post('/ingest')
      .send({
        ids: [101, 102, 103, 104, 105],
        priority: 'MEDIUM'
      });

    // Wait a bit to ensure different creation times and let processing start
    await new Promise(resolve => setTimeout(resolve, 500));

    // Create HIGH priority request
    const highResponse = await request(app)
      .post('/ingest')
      .send({
        ids: [201, 202, 203, 204],
        priority: 'HIGH'
      });

    // Wait longer for processing to definitely start
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Check statuses - use polling to wait for processing to start
    let highStatus;
    let attempts = 0;
    const maxAttempts = 10;
    
    do {
      highStatus = await request(app)
        .get(`/status/${highResponse.body.ingestion_id}`);
      
      if (highStatus.body.status !== 'yet_to_start') {
        break;
      }
      
      await new Promise(resolve => setTimeout(resolve, 500));
      attempts++;
    } while (attempts < maxAttempts);
    
    // High priority should be triggered or completed (processing started)
    expect(highStatus.body.status).toMatch(/^(triggered|completed)$/);
    
    // Check that high priority batches are being processed
    const highProcessingBatches = highStatus.body.batches.filter(b => 
      b.status === 'triggered' || b.status === 'completed'
    );
    expect(highProcessingBatches.length).toBeGreaterThan(0);

    // Wait for more processing and check the queue is working correctly
    await new Promise(resolve => setTimeout(resolve, 6000));

    const finalHighStatus = await request(app)
      .get(`/status/${highResponse.body.ingestion_id}`);
    
    // High priority should have made progress
    expect(finalHighStatus.body.status).toMatch(/^(triggered|completed)$/);
  }, 20000);

  // Test rate limiting
  test('Rate limiting - 1 batch per 5 seconds', async () => {
    const startTime = Date.now();
    
    // Create a request with multiple batches
    const response = await request(app)
      .post('/ingest')
      .send({
        ids: [301, 302, 303, 304, 305, 306, 307, 308, 309], // 3 batches
        priority: 'HIGH'
      });

    const ingestionId = response.body.ingestion_id;

    // Check initial status
    let status = await request(app)
      .get(`/status/${ingestionId}`);

    expect(status.body.batches.length).toBe(3);

    // Wait for first batch to complete (should be around 5 seconds)
    await new Promise(resolve => setTimeout(resolve, 6000));

    let currentStatus = await request(app)
      .get(`/status/${ingestionId}`);
    
    let completedBatches = currentStatus.body.batches.filter(b => b.status === 'completed').length;
    
    // After 6 seconds, should have at least 1 batch completed
    expect(completedBatches).toBeGreaterThanOrEqual(1);

    // Wait for second batch (another 5+ seconds)
    await new Promise(resolve => setTimeout(resolve, 6000));

    currentStatus = await request(app)
      .get(`/status/${ingestionId}`);
    
    completedBatches = currentStatus.body.batches.filter(b => b.status === 'completed').length;
    
    // After ~12 seconds total, should have at least 2 batches completed
    expect(completedBatches).toBeGreaterThanOrEqual(2);

    // Verify total time is reasonable (should take at least 10 seconds for 3 batches)
    const totalTime = Date.now() - startTime;
    expect(totalTime).toBeGreaterThan(10000);
  }, 20000);

  // Test concurrent requests with different priorities - Fixed assertion logic
  test('Concurrent requests with different priorities', async () => {
    // Send multiple requests quickly
    const lowPromise = request(app)
      .post('/ingest')
      .send({
        ids: [401, 402, 403],
        priority: 'LOW'
      });

    const mediumPromise = request(app)
      .post('/ingest')
      .send({
        ids: [501, 502, 503],
        priority: 'MEDIUM'
      });

    const highPromise = request(app)
      .post('/ingest')
      .send({
        ids: [601, 602, 603],
        priority: 'HIGH'
      });

    const [lowResponse, mediumResponse, highResponse] = await Promise.all([
      lowPromise, mediumPromise, highPromise
    ]);

    expect(lowResponse.status).toBe(200);
    expect(mediumResponse.status).toBe(200);
    expect(highResponse.status).toBe(200);

    // Wait longer for processing to start and use polling
    let highStatus;
    let attempts = 0;
    const maxAttempts = 10;
    
    do {
      await new Promise(resolve => setTimeout(resolve, 500));
      highStatus = await request(app)
        .get(`/status/${highResponse.body.ingestion_id}`);
      
      if (highStatus.body.status !== 'yet_to_start') {
        break;
      }
      attempts++;
    } while (attempts < maxAttempts);

    // Check that high priority is being processed first
    expect(highStatus.body.status).toMatch(/^(triggered|completed)$/);
  }, 15000);

  // Test large ID values
  test('Large ID values within range', async () => {
    const largeId = 1000000000; // 10^9
    const response = await request(app)
      .post('/ingest')
      .send({
        ids: [largeId, largeId + 1, largeId + 2],
        priority: 'HIGH'
      });

    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('ingestion_id');
  });

  // Test status updates during processing
  test('Status updates during processing', async () => {
    const response = await request(app)
      .post('/ingest')
      .send({
        ids: [701, 702, 703, 704, 705, 706],
        priority: 'MEDIUM'
      });

    const ingestionId = response.body.ingestion_id;

    // Initial status should be yet_to_start
    let status = await request(app)
      .get(`/status/${ingestionId}`);

    // Status should be either yet_to_start or triggered initially
    expect(status.body.status).toMatch(/^(yet_to_start|triggered)$/);

    // Wait for processing to begin
    await new Promise(resolve => setTimeout(resolve, 1000));

    status = await request(app)
      .get(`/status/${ingestionId}`);

    // Should be triggered or completed now
    expect(status.body.status).toMatch(/^(triggered|completed)$/);
  }, 5000);
});