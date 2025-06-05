const { v4: uuidv4 } = require('uuid');

class IngestionService {
  constructor() {
    this.ingestions = new Map();
    this.batchQueue = []; // Queue of individual batches, not ingestions
    this.isProcessing = false;
    this.BATCH_SIZE = 3;
    this.RATE_LIMIT_MS = 5000; // 5 seconds
    this.processingPromise = null;
  }

  // Create a new ingestion request
  createIngestion(ids, priority) {
    const ingestionId = uuidv4();
    const batches = this.createBatches(ids, ingestionId, priority);
    
    this.ingestions.set(ingestionId, {
      id: ingestionId,
      priority,
      batches,
      createdAt: new Date(),
      status: 'yet_to_start'
    });

    // Add all batches to the processing queue
    const createdAt = new Date();
    batches.forEach(batch => {
      this.batchQueue.push({
        ingestionId,
        batchId: batch.batchId,
        priority,
        createdAt,
        batch
      });
    });

    // Sort queue by priority and creation time
    this.sortQueue();

    // Start processing immediately (use setImmediate to ensure it starts right away)
    setImmediate(() => {
      this.startProcessing();
    });

    return ingestionId;
  }

  // Create batches of IDs
  createBatches(ids, ingestionId, priority) {
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
    this.batchQueue.sort((a, b) => {
      if (priorityOrder[a.priority] !== priorityOrder[b.priority]) {
        return priorityOrder[a.priority] - priorityOrder[b.priority];
      }
      return a.createdAt - b.createdAt;
    });
  }

  // Start processing (non-blocking)
  startProcessing() {
    if (!this.isProcessing && this.batchQueue.length > 0) {
      this.processingPromise = this.processQueue().catch(console.error);
    }
  }

  // Process the queue
  async processQueue() {
    if (this.isProcessing) return;

    this.isProcessing = true;
    
    try {
      while (this.batchQueue.length > 0) {
        // Always re-sort the queue before processing each batch
        // This ensures new high-priority requests jump to the front
        this.sortQueue();
        
        const queueItem = this.batchQueue.shift();
        const ingestion = this.ingestions.get(queueItem.ingestionId);
        
        if (!ingestion) {
          continue;
        }

        // Find the actual batch object in the ingestion
        const batch = ingestion.batches.find(b => b.batchId === queueItem.batchId);
        if (!batch || batch.status !== 'yet_to_start') {
          continue;
        }

        // Update ingestion status to triggered if it's yet to start
        if (ingestion.status === 'yet_to_start') {
          ingestion.status = 'triggered';
        }

        // Update batch status to triggered
        batch.status = 'triggered';

        // Process the batch (run it in background but don't wait for completion)
        this.processBatch(batch).then(() => {
          // Mark batch as completed
          batch.status = 'completed';

          // Check if all batches in this ingestion are completed
          const allCompleted = ingestion.batches.every(b => b.status === 'completed');
          if (allCompleted) {
            ingestion.status = 'completed';
          }
        }).catch(console.error);

        // Wait for rate limit (5 seconds between batches)
        if (this.batchQueue.length > 0) {
          await new Promise(resolve => setTimeout(resolve, this.RATE_LIMIT_MS));
        }
      }
    } finally {
      this.isProcessing = false;
      this.processingPromise = null;
    }
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

    // Determine overall status based on batch statuses
    let status = 'yet_to_start';
    const hasTriggered = ingestion.batches.some(batch => batch.status === 'triggered');
    const hasCompleted = ingestion.batches.some(batch => batch.status === 'completed');
    const allCompleted = ingestion.batches.every(batch => batch.status === 'completed');
    
    if (allCompleted) {
      status = 'completed';
    } else if (hasTriggered || hasCompleted) {
      status = 'triggered';
    }

    return {
      ingestion_id: ingestionId,
      status,
      batches: ingestion.batches.map(batch => ({
        batch_id: batch.batchId,
        ids: batch.ids,
        status: batch.status
      }))
    };
  }

  // Helper method to get current queue state (for testing)
  getQueueState() {
    return {
      queueLength: this.batchQueue.length,
      isProcessing: this.isProcessing,
      queue: this.batchQueue.map(item => ({
        ingestionId: item.ingestionId,
        batchId: item.batchId,
        priority: item.priority,
        createdAt: item.createdAt
      }))
    };
  }

  // Helper method to wait for current processing to complete (for testing)
  async waitForProcessing() {
    if (this.processingPromise) {
      await this.processingPromise;
    }
  }

  // Helper method to clear all data (for testing)
  clear() {
    this.ingestions.clear();
    this.batchQueue = [];
    this.isProcessing = false;
    this.processingPromise = null;
  }
}

// Export singleton instance
module.exports = new IngestionService();