# Temporal Order Workflow System

## Quick Start

### 1. Setup Environment
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Start Services
```bash
docker-compose build
docker-compose up
```

### 3. Initialize Database
```bash
# Create database tables (run this once)
docker-compose exec order-worker python migrations.py
```


### 4. TESTING
health check
```bash
curl http://localhost:8000/
```
Test order
```bash
curl -X POST "http://localhost:8000/orders/TEST-ORDER-1/start" \
  -H "Content-Type: application/json" \
  -d '{
    "payment_id": "pay-123"
  }'
```

curl -X POST "http://localhost:8000/orders/TEST-ORDER-1/signals/cancel" \
  -H "Content-Type: application/json"



## Services
- **Temporal UI**: http://localhost:8080
- **API Server**: http://localhost:8000
- **PostgreSQL**: localhost:5432

#make sure using most recent container
docker-compose ps

# set up the databases:
docker-compose exec order-worker python migrations.py




#########API TESTING#############
#make sure the API is running (Health checkpoint)
curl http://localhost:8000/

#test example workflow:
curl -X POST "http://localhost:8000/orders/TEST-1/start" \
  -H "Content-Type: application/json" \
  -d '{"payment_id": "pay-TEST-1"}'

#cancel order
curl -X POST "http://localhost:8000/orders/TEST-1/signals/cancel" \
  -H "Content-Type: application/json"


curl -X POST "http://localhost:8000/orders/TEST-7/start" \
  -H "Content-Type: application/json" \
  -d '{"payment_id": "pay-TEST-4"}'


curl -X POST "http://localhost:8000/orders/TEST-6/signals/update-address" \
  -H "Content-Type: application/json" \
  -d '{
    "street": "new street",
    "city": "new city",
    "state": "NY",
    "zip": "10001"
  }'



  curl -X GET "http://localhost:8000/orders/TEST-7/status" \
  -H "Content-Type: application/json"


  curl -X GET "http://localhost:8000/orders/TEST-6/events" \
  -H "Content-Type: application/json"

#CHECK DATABASE:



#connect to postgres
docker-compose exec postgresql psql -U temporal -d temporal

#watch logs in real time
docker-compose logs -f order-worker


curl -X GET "http://localhost:8000/" \
  -H "Content-Type: application/json"


TESTING:
pytest test_api_local.py -v                          