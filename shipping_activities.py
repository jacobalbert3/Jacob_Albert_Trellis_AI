from temporalio import activity
from typing import Dict, Any
from function_stubs import package_prepared, carrier_dispatched


class ShippingActivities:
    """Activities for shipping operations."""
    
    @activity.defn
    async def prepare_package(self, order: Dict[str, Any]) -> str:
        """Prepare package for shipping."""
        return await package_prepared(order)
    
    @activity.defn
    async def dispatch_carrier(self, order: Dict[str, Any]) -> str:
        """Dispatch package to carrier."""
        return await carrier_dispatched(order)
