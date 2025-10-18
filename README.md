# Trellis Take Home Instructions - JRA


### Setup
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Copy environmental variables
cp .env.example .env


# Install dependencies
pip install -r requirements.txt

# Build and start all services
docker-compose build
docker-compose up -d

# Initialize database (IMPORTANT: run before trying to submit any workflows)
docker-compose exec order-worker python migrations.py

# Verify services are running
docker-compose ps

# Test API health
curl http://localhost:8000/
```

### Watch Logs
```bash
# Watch worker logs
docker-compose logs -f order-worker
```

---

## Testing

### Option 1: Manual Testing with Curl

#### Basic Order Workflow
```bash
# Start an order
curl -X POST "http://localhost:8000/orders/TEST-1/start" \
  -H "Content-Type: application/json" \
  -d '{
    "payment_id": "pay-123"
  }'

# Check status
curl -X GET "http://localhost:8000/orders/TEST-1/status" \
  -H "Content-Type: application/json"

# Cancel order
curl -X POST "http://localhost:8000/orders/TEST-1/signals/cancel" \
  -H "Content-Type: application/json"

# Update address
curl -X POST "http://localhost:8000/orders/TEST-1/signals/update-address" \
  -H "Content-Type: application/json" \
  -d '{
    "street": "new street",
    "city": "new city",
    "state": "NY",
    "zip": "10001"
  }'
```

### Option 2: Automated Tests
```bash
# Run all tests
pytest test_api_local.py -v -s

# Run specific tests
pytest test_api_local.py::TestAPILocal::test_health_check -v -s
pytest test_api_local.py::TestAPILocal::test_complete_order_workflow -v -s
pytest test_api_local.py::TestAPILocal::test_cancel_order -v -s
pytest test_api_local.py::TestAPILocal::test_update_order_address -v -s
```

### Check Database
```bash
# Connect to database
docker-compose exec postgresql psql -U temporal -d temporal

# View orders
SELECT * FROM orders;

# View payments
SELECT * FROM payments;

# View events
SELECT * FROM events;
```

---


### Services Overview
- **Temporal Server**: `localhost:7233` - Workflow orchestration
- **Temporal UI**: `http://localhost:8080` - Web interface for monitoring
- **PostgreSQL**: `localhost:5432` - Database for persistence
- **API Server**: `http://localhost:8000` - REST API endpoints


### Database Initialization
```bash
# Create tables and schema
docker-compose exec order-worker python migrations.py

# Connect to database directly
docker-compose exec postgresql psql -U temporal -d temporal

# Check tables
docker-compose exec postgresql psql -U temporal -d temporal -c "\dt"
```

---

## How to Run Workers and Trigger Workflows

### Starting Workers
```bash
# Start the order worker (processes workflows)
docker-compose up -d order-worker

# Start the API server (handles HTTP requests)
docker-compose up -d order-api

# View worker logs
docker-compose logs -f order-worker
```

### Triggering Workflows

#### 1. Start an Order Workflow (note can change orderID and paymentID to whatever!)
```bash
curl -X POST "http://localhost:8000/orders/ORDER-123/start" \
  -H "Content-Type: application/json" \
  -d '{
    "payment_id": "pay-123"
  }'
```

#### 2. Check Workflow Status
```bash
curl -X GET "http://localhost:8000/orders/ORDER-123/status"
```
---

## How to Send Signals and Query/Inspect State

### Sending Signals

#### Cancel Order
```bash
curl -X POST "http://localhost:8000/orders/ORDER-123/signals/cancel"
```

#### Update Shipping Address
```bash
curl -X POST "http://localhost:8000/orders/ORDER-123/signals/update-address" \
  -H "Content-Type: application/json" \
  -d '{
    "street": "123 New Street",
    "city": "New City",
    "state": "CA",
    "zip": "90210"
  }'
```

### Querying and Inspecting State

#### Get Order Status
```bash
curl -X GET "http://localhost:8000/orders/ORDER-123/status"
```

Response includes:
- Workflow status (RUNNING, COMPLETED, FAILED, CANCELED)
- Current step in the workflow
- Order details (items, shipping address, payment info)
- Timestamps

#### Get Order Events
```bash
curl -X GET "http://localhost:8000/orders/ORDER-123/events"
```

Shows all events logged for the order:
- Workflow started
- Order received
- Payment charged
- Address updated
- Order canceled
- etc.

---

## 🗃️ Schema/Migrations and Persistence Rationale

### Database Schema

#### Tables
- **`orders`**: Order information and current state
- **`payments`**: Payment records with idempotency
- **`events`**: Audit trail of all workflow events

#### Key Design Decisions

**1. Idempotent Payments**
```sql
-- Payments table uses payment_id as primary key
-- Prevents duplicate charges for the same payment
CREATE TABLE payments (
    payment_id VARCHAR PRIMARY KEY,
    order_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    amount FLOAT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
```

**Idempotency Rationale:**
• **Payment Safety**: Uses `payment_id` as primary key to prevent duplicate charges - critical for financial operations
• **Temporal Retry Safety**: When Temporal retries activities due to timeouts/failures, payment won't be charged twice because of key
• **Database audit trail**: Each payment_id creates exactly one record in the database. If that order is cancelled, we can clean up those records as a followup (note this has not been done yet)

**Unique Key Strategy Across All Tables:**
• **Orders Table**: `id` (order_id) as primary key - prevents duplicate orders, ensures one order per unique identifier
• **Payments Table**: `payment_id` as primary key - prevents duplicate payment processing, critical for financial safety
• **Events Table**: `id` as auto-incrementing primary key - ensures each event has unique identifier for audit trail
• **Order State Consistency**: Each order_id can only have one current state, preventing conflicting state transitions
• **Event Uniqueness**: Each event gets unique ID even if same order_id has multiple events of same type
• **Temporal Workflow ID**: Each workflow uses `order-{order_id}` pattern ensuring unique workflow instances  
