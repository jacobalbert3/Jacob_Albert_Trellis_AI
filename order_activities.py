from temporalio import activity
from typing import Dict, Any
from function_stubs import order_received, order_validated, payment_charged, update_order_address


class OrderActivities:
    """Activities for order operations."""
    
    @activity.defn
    async def receive_order(self, order_id: str) -> Dict[str, Any]:
        """Receive and process a new order."""
        return await order_received(order_id)
    
    @activity.defn
    async def validate_order(self, order: Dict[str, Any]) -> bool:
        """Validate the order data."""
        return await order_validated(order)
    
    @activity.defn
    async def charge_payment(self, order: Dict[str, Any], payment_id: str) -> Dict[str, Any]:
        """Charge payment for the order with idempotency."""
        return await payment_charged(order, payment_id, None)
    
    @activity.defn
    async def update_address(self, order_id: str, new_address: Dict[str, Any]) -> Dict[str, Any]:
        """Update the shipping address for an order."""
        return await update_order_address(order_id, new_address)
