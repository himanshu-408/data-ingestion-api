const request = require('supertest');
const express = require('express');
const ingestRoutes = require('../routes/ingest');
const statusRoutes = require('../routes/status');

const app = express();
app.use(express.json());
app.use('/ingest', ingestRoutes);
app.use('/status', statusRoutes);

describe('Data Ingestion API Tests', () => {
  let ingestionId;

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
    ingestionId = response.body.ingestion_id;
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

  // Test status endpoint
  test('GET /status/:ingestionId', async () => {
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
});