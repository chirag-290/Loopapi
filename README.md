# Data Ingestion API

A robust RESTful API system for data ingestion with priority-based processing, rate limiting, and asynchronous batch processing built with Node.js, Express, and MongoDB.

## 🚀 Features

- **Priority-based Processing**: HIGH, MEDIUM, LOW priority queuing system
- **Rate Limiting**: Maximum 3 IDs processed every 5 seconds
- **Batch Processing**: IDs processed in batches of 3 asynchronously
- **Real-time Status Tracking**: Monitor ingestion progress in real-time
- **MongoDB Integration**: Persistent storage with Mongoose ODM
- **Comprehensive Testing**: Extensive test suite with 15+ test cases
- **Input Validation**: Robust validation for ID ranges and priority levels
- **Error Handling**: Graceful error handling throughout the application

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Client        │    │   Express API    │    │   MongoDB       │
│                 │    │                  │    │                 │
│ POST /ingest    │───▶│ • Input Validation│───▶│ • Ingestion Doc │
│ GET /status/:id │    │ • Batch Creation │    │ • Queue Items   │
└─────────────────┘    │ • Queue Management│    │ • Status Updates│
                       └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │ Background       │
                       │ Batch Processor  │
                       │ • Priority Queue │
                       │ • Rate Limiting  │
                       │ • Async Processing│
                       └──────────────────┘
```

## 📁 Project Structure

```
data-ingestion-api/
├── server.js                  # Main server entry point
├── package.json              # Dependencies and scripts
├── .env                      # Environment variables
├── .gitignore               # Git ignore rules
├── README.md                # This file
├── models/
│   └── Ingestion.js         # MongoDB schemas (Ingestion & Queue)
├── routes/
│   ├── ingest.js            # POST /ingest endpoint handler
│   └── status.js            # GET /status/:id endpoint handler
├── services/
│   ├── queueManager.js      # Priority queue management
│   └── batchProcessor.js    # Async batch processing logic
└── tests/
    └── api.test.js          # Comprehensive test suite
```

## 🛠️ Prerequisites

- **Node.js** (v14 or higher)
- **MongoDB** (v4.4 or higher)
- **npm** or **yarn**

## ⚡ Quick Start

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd data-ingestion-api
```

### 2. Install Dependencies

```bash
npm install
```

### 3. Setup Environment Variables

Create a `.env` file in the root directory:

```env
MONGODB_URI=mongodb://localhost:27017/data-ingestion
MONGODB_TEST_URI=mongodb://localhost:27017/data-ingestion-test
PORT=5000
```

### 4. Start MongoDB

Make sure MongoDB is running locally:

```bash
# Using MongoDB service
sudo systemctl start mongod

# Or using Docker
docker run -d -p 27017:27017 --name mongodb mongo:latest
```

### 5. Run the Application

```bash
# Development mode with auto-reload
npm run dev

# Production mode
npm start
```

The API will be available at `http://localhost:5000`

### 6. Run Tests

```bash
npm test
```

## 📚 API Documentation

### Base URL
```
http://localhost:5000
```

### Endpoints

#### 1. Submit Data Ingestion Request

**POST** `/ingest`

Submit a list of IDs for processing with specified priority.

**Request Body:**
```json
{
  "ids": [1, 2, 3, 4, 5],
  "priority": "HIGH"
}
```

**Parameters:**
- `ids` (array): List of integers (1 to 10^9+7)
- `priority` (string): "HIGH", "MEDIUM", or "LOW"

**Response:**
```json
{
  "ingestion_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Status Codes:**
- `200`: Success
- `400`: Invalid input
- `500`: Server error

#### 2. Check Ingestion Status

**GET** `/status/:ingestion_id`

Retrieve the processing status of an ingestion request.

**Response:**
```json
{
  "ingestion_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "triggered",
  "batches": [
    {
      "batch_id": "batch-uuid-1",
      "ids": [1, 2, 3],
      "status": "completed"
    },
    {
      "batch_id": "batch-uuid-2", 
      "ids": [4, 5],
      "status": "triggered"
    }
  ]
}
```

**Status Values:**
- **Overall Status**: `yet_to_start`, `triggered`, `completed`
- **Batch Status**: `yet_to_start`, `triggered`, `completed`

**Status Codes:**
- `200`: Success
- `404`: Ingestion not found
- `500`: Server error

#### 3. Health Check

**GET** `/health`

Check if the API is running.

**Response:**
```json
{
  "status": "OK",
  "timestamp": "2024-01-15T10:30:00.000Z"
}
```

## 🔧 Usage Examples

### Using cURL

```bash
# Submit ingestion request
curl -X POST http://localhost:5000/ingest \
  -H "Content-Type: application/json" \
  -d '{"ids": [1, 2, 3, 4, 5], "priority": "HIGH"}'

# Check status (replace with actual ingestion_id)
curl http://localhost:5000/status/550e8400-e29b-41d4-a716-446655440000

# Health check
curl http://localhost:5000/health
```

### Using JavaScript/Node.js

```javascript
// Submit ingestion request
const response = await fetch('http://localhost:5000/ingest', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({
    ids: [1, 2, 3, 4, 5],
    priority: 'HIGH'
  })
});

const { ingestion_id } = await response.json();

// Check status
const statusResponse = await fetch(`http://localhost:5000/status/${ingestion_id}`);
const status = await statusResponse.json();
console.log(status);
```

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017/data-ingestion` |
| `MONGODB_TEST_URI` | Test database connection | `mongodb://localhost:27017/data-ingestion-test` |
| `PORT` | Server port | `5000` |

### Processing Rules

1. **Batch Size**: IDs are processed in batches of 3
2. **Rate Limit**: Maximum 1 batch every 5 seconds
3. **Priority Order**: HIGH → MEDIUM → LOW
4. **Within Priority**: First-In-First-Out (FIFO)
5. **ID Range**: 1 to 10^9+7 (1,000,000,007)

## 🧪 Testing

The project includes a comprehensive test suite covering:

- ✅ **Basic Functionality**: Ingestion and status endpoints
- ✅ **Input Validation**: Invalid IDs, priorities, and edge cases
- ✅ **Priority Processing**: HIGH priority processed before MEDIUM/LOW
- ✅ **Rate Limiting**: Respects 1 batch per 5 seconds limit
- ✅ **Batch Creation**: Correct batching of IDs
- ✅ **Status Updates**: Real-time status tracking
- ✅ **Error Handling**: Graceful error responses
- ✅ **Edge Cases**: Large arrays, single IDs, maximum values

### Running Tests

```bash
# Run all tests
npm test

# Run tests with coverage
npm test -- --coverage

# Run specific test file
npm test -- api.test.js
```

### Test Results Example

```
✓ should accept valid ingestion request
✓ should reject invalid priority
✓ should reject empty ids array
✓ should reject ids outside valid range
✓ should create correct number of batches
✓ should return status for valid ingestion_id
✓ should return 404 for invalid ingestion_id
✓ should show correct batch structure
✓ should process HIGH priority before MEDIUM priority
✓ should respect rate limit of 1 batch per 5 seconds
✓ should handle large number of IDs
✓ should handle single ID
✓ should handle maximum valid ID

Test Suites: 1 passed, 1 total
Tests: 13 passed, 13 total
```

## 🚀 Deployment

### Heroku

1. **Create Heroku App**
```bash
heroku create your-app-name
```

2. **Add MongoDB Atlas**
```bash
# Add MongoDB Atlas add-on or set MONGODB_URI config var
heroku config:set MONGODB_URI=your-mongodb-connection-string
```

3. **Deploy**
```bash
git add .
git commit -m "Deploy to Heroku"
git push heroku main
```

### Railway

1. **Connect Repository**: Link your GitHub repository
2. **Set Environment Variables**: Add `MONGODB_URI` in settings
3. **Deploy**: Railway will automatically deploy

### Docker

```dockerfile
FROM node:16-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --only=production

COPY . .

EXPOSE 5000

CMD ["npm", "start"]
```

```bash
# Build and run
docker build -t data-ingestion-api .
docker run -p 5000:5000 -e MONGODB_URI=your-uri data-ingestion-api
```

## 🔍 Monitoring and Debugging

### Health Check

Monitor API health using the `/health` endpoint:

```bash
curl http://localhost:5000/health
```

### Logs

The application logs important events:

- Batch processing start/completion
- Queue additions
- Database connections
- Error occurrences

### Database Monitoring

Monitor MongoDB collections:

```javascript
// Check ingestion documents
db.ingestions.find({}).limit(5)

// Check queue items
db.queueitems.find({}).limit(5)

// Check processing status
db.ingestions.aggregate([
  { $group: { _id: "$status", count: { $sum: 1 } } }
])
```

## 🐛 Troubleshooting

### Common Issues

1. **MongoDB Connection Error**
   ```
   Solution: Ensure MongoDB is running and MONGODB_URI is correct
   ```

2. **Port Already in Use**
   ```bash
   # Kill process on port 5000
   sudo lsof -t -i tcp:5000 | xargs kill -9
   ```

3. **Test Failures**
   ```
   Solution: Ensure test database is accessible and clean
   ```

4. **Rate Limiting Not Working**
   ```
   Solution: Check system time and batch processor logs
   ```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make changes and add tests
4. Run tests: `npm test`
5. Commit changes: `git commit -m "Add feature"`
6. Push to branch: `git push origin feature-name`
7. Create a Pull Request

## 📋 API Rate Limits

| Endpoint | Rate Limit | Notes |
|----------|------------|-------|
| `POST /ingest` | No limit | Processing is rate-limited, not submission |
| `GET /status/:id` | No limit | Read-only operation |
| `GET /health` | No limit | Health check endpoint |

**Processing Rate Limit**: 3 IDs per 5 seconds (1 batch per 5 seconds)

## 🔐 Security Considerations

- **Input Validation**: All inputs are validated before processing
- **No Authentication**: As per requirements, no auth is implemented
- **Rate Limiting**: Processing rate limits prevent system overload
- **Error Handling**: Sensitive information is not exposed in errors
- **CORS**: Configured for cross-origin requests

## 📊 Performance

- **Concurrent Processing**: Handles multiple ingestion requests simultaneously
- **Efficient Queuing**: Priority-based queue with optimal sorting
- **Database Indexing**: Proper indexing on frequently queried fields
- **Memory Management**: Efficient memory usage with streaming where possible

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📞 Support

For questions or issues:

1. Check the troubleshooting section
2. Review the test cases for usage examples
3. Open an issue on GitHub
4. Contact the development team

---

**Built with ❤️ using Node.js, Express, and MongoDB**
