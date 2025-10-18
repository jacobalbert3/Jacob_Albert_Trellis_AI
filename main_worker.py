import asyncio
import concurrent.futures
import os
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from order_workflow import OrderWorkflow
from order_activities import OrderActivities
from shipping_workflow import ShippingWorkflow
from shipping_activities import ShippingActivities
from database import db_manager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Reduce Temporal's verbose error logging
logging.getLogger("temporalio.activity").setLevel(logging.WARNING)
logging.getLogger("temporalio.worker._workflow_instance").setLevel(logging.WARNING)

async def main():
    """Main worker that handles both order and shipping workflows."""
    
    # Use environment variable for Temporal address, fallback to localhost for local development
    temporal_address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    logger.info(f"Connecting to Temporal at {temporal_address}")
    
    client = await Client.connect(temporal_address, namespace="default")

    # Create activities instances
    order_activities = OrderActivities()
    shipping_activities = ShippingActivities()
    
    # Create workers for different task queues
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as activity_executor:
        
        # Main order worker
        order_worker = Worker(
            client, 
            task_queue="order-tq", 
            workflows=[OrderWorkflow], 
            activities=[
                order_activities.receive_order,
                order_activities.validate_order,
                order_activities.charge_payment,
                order_activities.update_address
            ],
            activity_executor=activity_executor,
        )
        
        # Shipping worker
        shipping_worker = Worker(
            client, 
            task_queue="shipping-tq", 
            workflows=[ShippingWorkflow], 
            activities=[
                shipping_activities.prepare_package,
                shipping_activities.dispatch_carrier
            ],
            activity_executor=activity_executor,
        )
        
        logger.info("Order worker listening on task queue: order-tq")
        logger.info("Shipping worker listening on task queue: shipping-tq")
        
        # Run both workers concurrently
        await asyncio.gather(
            order_worker.run(),
            shipping_worker.run()
        )

if __name__ == "__main__":
    asyncio.run(main())
