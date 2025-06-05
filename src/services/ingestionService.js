const { v4: uuidv4 } = require('uuid');

class IngestionService {
  constructor() {
    this.ingestions = new Map();
    this.processingQueue = [];
    this.isProcessing = false;
    this.BATCH_SIZE = 3;
    this.RATE_LIMIT_MS = 5000; // 5 seconds
  }

  // Create a new ingestion request
  createIngestion(ids, priority) {
    const ingestionId = uuidv4();
    const batches = this.createBatches(ids);
    
    this.ingestions.set(ingestionId, {
      id: ingestionId,
      priority,
      batches,
      createdAt: new Date(),
      status: 'yet_to_start'
    });

    // Add to processing queue
    this.processingQueue.push({
      ingestionId,
      priority,
      createdAt: new Date()
    });

    // Sort queue by priority and creation time
    this.sortQueue();

    // Start processing if not already processing
    if (!this.isProcessing) {
      this.processQueue();
    }

    return ingestionId;
  }

  // Create batches of IDs
  createBatches(ids) {
    const batches = [];
    for (let i = 0; i < ids.length; i += this.BATCH_SIZE) {
      const batchIds = ids.slice(i, i + this.BATCH_SIZE);
      batches.push({
        batchId: uuidv4(),
        ids: batchIds,
        status: 'yet_to_start'
      });
    }
    return batches;
  }

  // Sort queue by priority and creation time
  sortQueue() {
    const priorityOrder = { HIGH: 0, MEDIUM: 1, LOW: 2 };
    this.processingQueue.sort((a, b) => {
      if (priorityOrder[a.priority] !== priorityOrder[b.priority]) {
        return priorityOrder[a.priority] - priorityOrder[b.priority];
      }
      return a.createdAt - b.createdAt;
    });
  }

  // Process the queue
  async processQueue() {
    if (this.isProcessing || this.processingQueue.length === 0) return;

    this.isProcessing = true;
    
    while (this.processingQueue.length > 0) {
      // Always re-sort the queue before each batch
      this.sortQueue();
      const { ingestionId } = this.processingQueue[0];
      const ingestion = this.ingestions.get(ingestionId);
      
      if (!ingestion) {
        this.processingQueue.shift();
        continue;
      }

      // Update ingestion status to triggered if it's yet to start
      if (ingestion.status === 'yet_to_start') {
        ingestion.status = 'triggered';
      }

      // Process next batch
      const nextBatch = ingestion.batches.find(batch => batch.status === 'yet_to_start');
      if (nextBatch) {
        nextBatch.status = 'triggered';
        await this.processBatch(nextBatch);
        nextBatch.status = 'completed';
      }

      // Check if all batches are completed
      const allCompleted = ingestion.batches.every(batch => batch.status === 'completed');
      if (allCompleted) {
        ingestion.status = 'completed';
        this.processingQueue.shift();
      }

      // Wait for rate limit
      await new Promise(resolve => setTimeout(resolve, this.RATE_LIMIT_MS));
    }

    this.isProcessing = false;
  }

  // Process a single batch
  async processBatch(batch) {
    // Simulate external API calls for each ID
    const promises = batch.ids.map(async (id) => {
      // Simulate API delay (random between 100-500ms)
      await new Promise(resolve => setTimeout(resolve, Math.random() * 400 + 100));
      return { id, data: 'processed' };
    });

    await Promise.all(promises);
  }

  // Get ingestion status
  getIngestionStatus(ingestionId) {
    const ingestion = this.ingestions.get(ingestionId);
    if (!ingestion) return null;

    // Determine overall status
    let status = 'yet_to_start';
    if (ingestion.batches.some(batch => batch.status === 'triggered')) {
      status = 'triggered';
    }
    if (ingestion.batches.every(batch => batch.status === 'completed')) {
      status = 'completed';
    }

    return {
      ingestion_id: ingestionId,
      status,
      batches: ingestion.batches
    };
  }
}

// Export singleton instance
module.exports = new IngestionService(); 