const express = require('express');
const cors = require('cors');
const ingestRoutes = require('./routes/ingest');
const statusRoutes = require('./routes/status');

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());

// Routes
app.use('/ingest', ingestRoutes);
app.use('/status', statusRoutes);

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    error: 'Something went wrong!',
    message: err.message
  });
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
}); 