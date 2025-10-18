import asyncio
import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from temporalio.client import Client
# from temporalio.exceptions import WorkflowNotFoundError

from database import db_manager, Order, Payment, Event

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Temporal Order Workflow API", version="1.0.0")

# Global Temporal client
temporal_client: Optional[Client] = None

class OrderStartRequest(BaseModel):
    payment_id: str

class AddressUpdateRequest(BaseModel):
    street: str
    city: str
    state: str
    zip: str

class OrderStatusResponse(BaseModel):
    order_id: str
    workflow_id: str
    status: str
    current_step: str
    created_at: datetime
    updated_at: datetime
    items: list
    shipping_address: dict
    payment_info: Optional[dict] = None

@app.on_event("startup")
async def startup_event():
    """Initialize database and Temporal client on startup."""
    global temporal_client
    
    # Initialize database connections (tables should already exist from migrations.py)
    logger.info("Connecting to database...")
    db_manager.create_engine_and_session()
    db_manager.create_async_engine_and_session()
    logger.info("Database connected successfully")
    
    # Initialize Temporal client
    temporal_address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    logger.info(f"Connecting to Temporal at {temporal_address}")
    temporal_client = await Client.connect(temporal_address, namespace="default")
    logger.info("Temporal client connected successfully")

@app.get("/")
async def root():
    """Health check endpoint."""
    return {"message": "Temporal Order Workflow API is running"}

@app.post("/orders/{order_id}/start")
async def start_order_workflow(order_id: str, request: OrderStartRequest):
    """Start OrderWorkflow with a provided payment_id."""
    if not temporal_client:
        raise HTTPException(status_code=500, detail="Temporal client not initialized")
    
    try:
        # Start the workflow
        from order_workflow import OrderWorkflow
        
        handle = await temporal_client.start_workflow(
            OrderWorkflow.run,
            args=[order_id, request.payment_id],
            id=f"order-{order_id}",
            task_queue="order-tq",
        )
        
        logger.info(f"Started order workflow {handle.id} for order {order_id}")
        
        # Log the event
        _log_event(order_id, "workflow_started", {
            "workflow_id": handle.id,
            "payment_id": request.payment_id
        })
        
        return {
            "order_id": order_id,
            "workflow_id": handle.id,
            "run_id": handle.result_run_id,
            "status": "started"
        }
        
    except Exception as e:
        logger.error(f"Error starting workflow for order {order_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start workflow: {str(e)}")

@app.post("/orders/{order_id}/signals/cancel")
async def cancel_order(order_id: str):
    """Send the CancelOrder signal to the workflow."""
    if not temporal_client:
        raise HTTPException(status_code=500, detail="Temporal client not initialized")
    
    try:
        workflow_id = f"order-{order_id}"
        
        # Get workflow handle
        handle = temporal_client.get_workflow_handle(workflow_id)
        
        # Send cancel signal
        await handle.signal("cancel_order_signal")
        
        logger.info(f"Sent cancel signal to workflow {workflow_id}")
        
        # Log the event
        _log_event(order_id, "order_cancelled", {"workflow_id": workflow_id})
        
        return {
            "order_id": order_id,
            "workflow_id": workflow_id,
            "status": "cancellation_signal_sent"
        }
        
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(status_code=404, detail=f"Workflow for order {order_id} not found")
        else:
            logger.error(f"Error cancelling order {order_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to cancel order: {str(e)}")

@app.post("/orders/{order_id}/signals/update-address")
async def update_address(order_id: str, request: AddressUpdateRequest):
    """Send the UpdateAddress signal to the workflow."""
    if not temporal_client:
        raise HTTPException(status_code=500, detail="Temporal client not initialized")
    
    try:
        workflow_id = f"order-{order_id}"
        
        # Get workflow handle
        handle = temporal_client.get_workflow_handle(workflow_id)
        
        # Prepare address data
        new_address = {
            "street": request.street,
            "city": request.city,
            "state": request.state,
            "zip": request.zip
        }
        
        # Send address update signal
        await handle.signal("update_address_signal", new_address)
        
        logger.info(f"Sent address update signal to workflow {workflow_id}")
        
        # Log the event
        _log_event(order_id, "address_updated", {
            "workflow_id": workflow_id,
            "new_address": new_address
        })
        
        return {
            "order_id": order_id,
            "workflow_id": workflow_id,
            "status": "address_update_signal_sent",
            "new_address": new_address
        }
        
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(status_code=404, detail=f"Workflow for order {order_id} not found")
        else:
            logger.error(f"Error updating address for order {order_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to update address: {str(e)}")

@app.get("/orders/{order_id}/status")
async def get_order_status(order_id: str):
    """Query OrderWorkflow status to retrieve current state."""
    if not temporal_client:
        raise HTTPException(status_code=500, detail="Temporal client not initialized")
    
    try:
        workflow_id = f"order-{order_id}"
        
        # Get workflow handle
        handle = temporal_client.get_workflow_handle(workflow_id)
        
        # Get workflow status
        workflow_status = await handle.describe()
        
        # Query workflow for current state (this is the key addition!)
        workflow_state = None
        try:
            workflow_state = await handle.query("status")
        except Exception as e:
            logger.warning(f"Could not query workflow state: {e}")
        
        # Get order data from database
        session = db_manager.get_session()
        try:
            db_order = session.query(Order).filter(Order.id == order_id).first()
            if not db_order:
                raise HTTPException(status_code=404, detail=f"Order {order_id} not found in database")
            
            # Get payment info if available
            payment_info = None
            payment = session.query(Payment).filter(Payment.order_id == order_id).first()
            if payment:
                payment_info = {
                    "payment_id": payment.payment_id,
                    "status": payment.status,
                    "amount": payment.amount,
                    "created_at": payment.created_at
                }
            
            # Use workflow query result if available, otherwise fall back to database
            current_step = workflow_state.get("current_step") if workflow_state else db_order.state
            
            return OrderStatusResponse(
                order_id=order_id,
                workflow_id=workflow_id,
                status=workflow_status.status.name,
                current_step=current_step,
                created_at=db_order.created_at,
                updated_at=db_order.updated_at,
                items=db_order.items or [],
                shipping_address=db_order.shipping_address or {},
                payment_info=payment_info
            )
            
        finally:
            session.close()
        
    except Exception as e:
        if "not found" in str(e).lower():
            raise HTTPException(status_code=404, detail=f"Workflow for order {order_id} not found")
        else:
            logger.error(f"Error getting status for order {order_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to get order status: {str(e)}")

@app.get("/orders/{order_id}/events")
async def get_order_events(order_id: str):
    """Get all events for an order."""
    session = db_manager.get_session()
    try:
        events = session.query(Event).filter(Event.order_id == order_id).order_by(Event.timestamp.desc()).all()
        
        return {
            "order_id": order_id,
            "events": [
                {
                    "id": event.id,
                    "event_type": event.event_type,
                    "payload": event.payload_json,
                    "timestamp": event.timestamp
                }
                for event in events
            ]
        }
        
    finally:
        session.close()

def _log_event(order_id: str, event_type: str, payload: Dict[str, Any]):
    """Log an event to the database."""
    session = db_manager.get_session()
    try:
        event = Event(
            order_id=order_id,
            event_type=event_type,
            payload_json=payload
        )
        session.add(event)
        session.commit()
    except Exception as e:
        logger.error(f"Error logging event: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
