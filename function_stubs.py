from typing import Dict, Any
from database import db_manager, Order, Payment, Event
from sqlalchemy import select
import asyncio
import random
import logging
from datetime import datetime

# Set up logging for activities - use same logger as worker
logger = logging.getLogger("__main__")


async def flaky_call() -> None:
    """Either raise an error or sleep long enough to trigger an activity timeout."""
    rand_num = random.random()
    if rand_num < 0.33:
        logger.info("!!FLAKY CALL: Forced failure (33% chance)")
        raise RuntimeError("Forced failure for testing")
    
    if rand_num < 0.67:
        logger.info("!!FLAKY CALL: Simulating timeout (33% chance)")
        await asyncio.sleep(300)
    
    # Success case (33% chance)
    logger.info("!!FLAKY CALL: Success (33% chance)")
		
		
async def order_received(order_id: str) -> Dict[str, Any]:
    logger.info(f"ACTIVITY: Starting order_received for {order_id}")
    await flaky_call()
    
    # Hardcoded order data
    order_data = {
        "order_id": order_id,
        "state": "received",
        "items": [
            {"sku": "ABC", "qty": 1},
            {"sku": "XYZ", "qty": 2}
        ],
        "shipping_address": {
            "street": "123 Main St",
            "city": "Default City", 
            "state": "DC",
            "zip": "12345"
        }
    }
    
    # Insert order into database
    async_session = db_manager.get_async_session()
    try:
        # Check if order already exists (idempotency)
        result = await async_session.execute(
            select(Order).where(Order.id == order_id)
        )
        existing_order = result.scalar_one_or_none()
        
        if existing_order:
            # Order already exists, return existing data (idempotent)
            print(f"Order {order_id} already exists (idempotent)")
            return {
                "order_id": existing_order.id,
                "state": existing_order.state,
                "items": existing_order.items,
                "shipping_address": existing_order.shipping_address
            }
        
        # Create new Order record
        new_order = Order(
            id=order_id,
            state="received",
            items=order_data["items"],
            shipping_address=order_data["shipping_address"]
        )
        
        # Add to session and commit
        async_session.add(new_order)
        await async_session.commit()
        print(f"Order {order_id} inserted into database successfully")
        
    except Exception as e:
        await async_session.rollback()
        print(f"Error inserting order {order_id}: {e}")
        raise
    finally:
        await async_session.close()
    
    return order_data

async def order_validated(order: Dict[str, Any]) -> bool:
    order_id = order.get("order_id", "unknown")
    logger.info(f"ACTIVITY: Starting order_validated for {order_id}")
    await flaky_call()
    
    # Fetch order from database and update validation status with idempotency
    async_session = db_manager.get_async_session()
    try:
        order_id = order.get("order_id")
        if not order_id:
            raise ValueError("Order ID is required")
        
        # Fetch order from database using async
        result = await async_session.execute(
            select(Order).where(Order.id == order_id)
        )
        db_order = result.scalar_one_or_none()
        
        if not db_order:
            raise ValueError(f"Order {order_id} not found in database")
        
        # Check if already validated (idempotency)
        if db_order.state == "validated":
            print(f"Order {order_id} already validated (idempotent)")
            return True
        
        # Validate order data
        if not order.get("items"):
            raise ValueError("No items to validate")
        
        # Update order state to validated
        db_order.state = "validated"
        await async_session.commit()
        print(f"Order {order_id} validated successfully")
        
        return True
        
    except Exception as e:
        await async_session.rollback()
        print(f"Error validating order: {e}")
        raise
    finally:
        await async_session.close()

async def payment_charged(order: Dict[str, Any], payment_id: str, db) -> Dict[str, Any]:
    """Charge payment"""
    order_id = order.get("order_id", "unknown")
    logger.info(f"ACTIVITY: Starting payment_charged for {order_id} (payment_id: {payment_id})")
    
    # Calculate amount
    amount = sum(i.get("qty", 1) for i in order.get("items", []))
    
    async_session = db_manager.get_async_session()
    try:
        # Check if payment already exists
        result = await async_session.execute(
            select(Payment).where(Payment.payment_id == payment_id)
        )
        existing_payment = result.scalar_one_or_none()
        
        if existing_payment:
            # Payment already processed, return existing result
            print(f"Payment {payment_id} already processed (idempotent)")
            return {
                "status": existing_payment.status,
                "amount": existing_payment.amount,
                "payment_id": payment_id
            }
        
        # EXTERNAL SIDE EFFECT: Call payment service (simulated with flaky_call)
        await flaky_call()  
        
        # RECORD EXTERNAL SIDE EFFECT AFTER IT SUCCEEDS

        new_payment = Payment(
            payment_id=payment_id,
            order_id=order.get("order_id"),
            status="charged",
            amount=amount
        )
        
        async_session.add(new_payment)
        await async_session.commit()

        
        return {
            "status": "charged",
            "amount": amount,
            "payment_id": payment_id
        }
        
    except Exception as e:
        await async_session.rollback()
        print(f"Error charging payment {payment_id}: {e}")
        raise
    finally:
        await async_session.close()

async def order_shipped(order: Dict[str, Any]) -> str:
    await flaky_call()
    
    # Update order status
    async_session = db_manager.get_async_session()
    try:
        order_id = order.get("order_id")
        if not order_id:
            raise ValueError("Order ID is required")
        
        # Fetch and update order
        result = await async_session.execute(
            select(Order).where(Order.id == order_id)
        )
        db_order = result.scalar_one_or_none()
        
        if not db_order:
            raise ValueError(f"Order {order_id} not found in database")
        
        # Check if already shipped
        if db_order.state == "shipped":
            print(f"Order {order_id} already shipped (idempotent)")
            return "Shipped"
        
        db_order.state = "shipped"
        await async_session.commit()
        print(f"Order {order_id} marked as shipped")
        
        return "Shipped"
        
    except Exception as e:
        await async_session.rollback()
        print(f"Error updating order status: {e}")
        raise
    finally:
        await async_session.close()

async def package_prepared(order: Dict[str, Any]) -> str:
    order_id = order.get("order_id", "unknown")
    logger.info(f"ACTIVITY: Starting package_prepared for {order_id}")
    await flaky_call()
    
    # Mark package as prepared in database with idempotency
    async_session = db_manager.get_async_session()
    try:
        order_id = order.get("order_id")
        if not order_id:
            raise ValueError("Order ID is required")
        
        # Fetch and update order using async
        result = await async_session.execute(
            select(Order).where(Order.id == order_id)
        )
        db_order = result.scalar_one_or_none()
        
        if not db_order:
            raise ValueError(f"Order {order_id} not found in database")
        
        # Check if already prepared
        if db_order.state == "package_prepared":
            print(f"Package for order {order_id} already prepared (idempotent)")
            return "Package ready"
        
        db_order.state = "package_prepared"
        await async_session.commit()
        print(f"Package for order {order_id} prepared successfully")
        
        return "Package ready"
        
    except Exception as e:
        await async_session.rollback()
        print(f"Error preparing package: {e}")
        raise
    finally:
        await async_session.close()

async def carrier_dispatched(order: Dict[str, Any]) -> str:
    order_id = order.get("order_id", "unknown")
    logger.info(f"ACTIVITY: Starting carrier_dispatched for {order_id}")
    
    # Record carrier dispatch status in database
    async_session = db_manager.get_async_session()
    try:
        order_id = order.get("order_id")
        if not order_id:
            raise ValueError("Order ID is required")
        
        # Fetch and update order
        result = await async_session.execute(
            select(Order).where(Order.id == order_id)
        )
        db_order = result.scalar_one_or_none()
        
        if not db_order:
            raise ValueError(f"Order {order_id} not found in database")
        
        # Check if already dispatched
        if db_order.state == "dispatched":
            print(f"Carrier for order {order_id} already dispatched (idempotent)")
            return "Dispatched"
        

        await flaky_call()  # This simulates the external carrier service call
        

        db_order.state = "dispatched"
        await async_session.commit()
        
        return "Dispatched"
        
    except Exception as e:
        await async_session.rollback()
        print(f"Error dispatching carrier: {e}")
        raise
    finally:
        await async_session.close()


async def update_order_address(order_id: str, new_address: Dict[str, Any]) -> Dict[str, Any]:
    """Update shipping address for an order with idempotency."""
    logger.info(f"ACTIVITY: Starting update_order_address for {order_id}")
    
    async_session = db_manager.get_async_session()
    try:
        # Check if order exists
        result = await async_session.execute(
            select(Order).where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()
        
        if not order:
            raise ValueError(f"Order {order_id} not found")
        
        # Update shipping address
        order.shipping_address = new_address
        order.updated_at = datetime.utcnow()
        
        await async_session.commit()
        
        logger.info(f"ACTIVITY: Address updated for {order_id} to {new_address}")
        
        return {
            "order_id": order_id,
            "updated_address": new_address,
            "status": "address_updated"
        }
        
    except Exception as e:
        await async_session.rollback()
        logger.error(f"Error updating address for order {order_id}: {e}")
        raise
    finally:
        await async_session.close()