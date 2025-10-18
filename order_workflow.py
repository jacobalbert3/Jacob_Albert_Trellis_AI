from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ChildWorkflowError, ApplicationError
from datetime import timedelta
from typing import Dict, Any

# Import activities, passing it through the sandbox without reloading the module
with workflow.unsafe.imports_passed_through():
    from shipping_workflow import ShippingWorkflow
    from order_activities import OrderActivities

@workflow.defn
class OrderWorkflow:
    """Main workflow that orchestrates the entire order lifecycle."""
    
    def __init__(self):
        self.cancelled = False
        self.updated_address = None
        self.current_step = "initialized"
        self.order_id = None
        self.payment_id = None
        self.last_dispatch_failure = None
    
    # tells temporal we can have external signals
    @workflow.signal
    async def cancel_order_signal(self):
        """Signal to cancel order."""
        self.cancelled = True
    
    @workflow.signal
    async def update_address_signal(self, new_address: Dict[str, Any]):
        """Signal to update shipping address."""
        self.updated_address = new_address
    
    @workflow.signal
    async def dispatch_failed(self, reason: str):
        """Child calls this when dispatch fails."""
        self.last_dispatch_failure = reason
        self.current_step = "shipping_dispatch_failed"
        workflow.logger.error(f"📣 SIGNAL from child: dispatch_failed -> {reason}")
    
    @workflow.query
    def status(self) -> Dict[str, Any]:
        """Query to get current workflow status."""
        return {
            "order_id": self.order_id,
            "payment_id": self.payment_id,
            "current_step": self.current_step,
            "cancelled": self.cancelled,
            "updated_address": self.updated_address,
            "workflow_type": "OrderWorkflow"
        }
    
    @workflow.run
    async def run(self, order_id: str, payment_id: str) -> str:
        """ main order workflow exec"""
        # Store workflow parameters
        self.order_id = order_id
        self.payment_id = payment_id
        
        # Use workflow.logger instead of print for better visibility
        workflow.logger.info(f"WORKFLOW START: OrderWorkflow started for {order_id} (payment_id: {payment_id})")
        
        # run the main steps of workflow
        result = await self._execute_order_flow(order_id, payment_id)
        
        workflow.logger.info(f"WORKFLOW END: OrderWorkflow completed for {order_id} with result: {result}")
        return result
    



    async def _execute_order_flow(self, order_id: str, payment_id: str) -> str:
        # Step 1: Receive the order
        self.current_step = "receiving_order"
        workflow.logger.info(f"WORKFLOW STEP: Starting order receipt for {order_id}")
        
        try:
            order = await workflow.execute_activity_method(
                OrderActivities.receive_order,
                args=[order_id],
                start_to_close_timeout=timedelta(seconds=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(milliseconds=100),
                    backoff_coefficient=1.0,            # no growth
                    maximum_interval=timedelta(milliseconds=100),
                    maximum_attempts=5  
                ),
            )
            workflow.logger.info(f"✅ WORKFLOW STEP: Order {order_id} received successfully")

        except Exception as e:
            workflow.logger.error(f"❌ WORKFLOW STEP: Order receipt failed for {order_id}: {str(e)}")

            return f"Order workflow failed at receive_order: {str(e)}"
        
        # Check for cancellation
        if self.cancelled:
            workflow.logger.info(f"Order cancelled!")
            return "Order cancelled after receipt"
        
        # Step 2: Validate order
        self.current_step = "validating_order"
        workflow.logger.info(f"WORKFLOW STEP: Starting order validation for {order_id}")
        try:
            is_valid = await workflow.execute_activity_method(
                OrderActivities.validate_order,
                args=[order],
                start_to_close_timeout=timedelta(seconds=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(milliseconds=100),
                    backoff_coefficient=1.0,            # no growth
                    maximum_interval=timedelta(milliseconds=100),  # clamp at 1s
                    maximum_attempts=5  
                ),
            )
            if not is_valid:
                workflow.logger.error(f"WORKFLOW STEP: Order validation failed for {order_id}")
                return "Order validation failed"
            workflow.logger.info(f"WORKFLOW STEP: Order {order_id} validated successfully")
        except Exception as e:
            workflow.logger.error(f"WORKFLOW STEP: Order validation failed for {order_id}: {str(e)}")
            return f"Order workflow failed at validate_order: {str(e)}"
        
        # Check for cancellation after validation
        if self.cancelled:
            workflow.logger.info(f"Order cancelled!")
            return "Order cancelled after validation"
        
        # Step 3: Manual review timer (simulated human approval)
        self.current_step = "manual_review"
        workflow.logger.info(f"WORKFLOW STEP: Manual review timer for {order_id} (2 seconds)")
        await workflow.sleep(timedelta(seconds=1))  # Simulate 2-second review time
        workflow.logger.info(f"WORKFLOW STEP: Manual review completed for {order_id}")
        
        # Check for cancellation after manual review
        if self.cancelled:
            workflow.logger.info(f"Order cancelled!")
            return "Order cancelled after manual review"
        
        # Step 4: Charge payment
        self.current_step = "charging_payment"
        workflow.logger.info(f"WORKFLOW STEP: Starting payment charge for {order_id} (payment_id: {payment_id})")
        try:
            payment_result = await workflow.execute_activity_method(
                OrderActivities.charge_payment,
                args=[order, payment_id],
                start_to_close_timeout=timedelta(seconds=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(milliseconds=100),
                    backoff_coefficient=1.0,            # no growth
                    maximum_interval=timedelta(milliseconds=100),  # clamp at 1s
                    maximum_attempts=5  
                ),
            )
            # Validate payment was successful
            if payment_result.get("status") != "charged":
                workflow.logger.error(f" WORKFLOW STEP: Payment failed for {order_id}: {payment_result}")
                return f"Payment failed: {payment_result}"
            workflow.logger.info(f" WORKFLOW STEP: Payment charged successfully for {order_id}")
        except Exception as e:
            workflow.logger.error(f" WORKFLOW STEP: Payment charge failed for {order_id}: {str(e)}")
            return f"Order workflow failed at charge_payment: {str(e)}"
        
        # Check for cancellation before shipping
        if self.cancelled:
            workflow.logger.info(f"Order cancelled!")
            return "Order cancelled before shipping"
        




        
        # Step 5: Start shipping workflow as child workflow with built-in retry policy
        self.current_step = "shipping"
        workflow.logger.info(f"WORKFLOW STEP: Starting shipping workflow for {order_id}")
        


        # Check if address was updated and call activity to update database
        if self.updated_address:
            workflow.logger.info(f"Updating shipping address to {self.updated_address}")
            try:
                update_result = await workflow.execute_activity_method(
                    OrderActivities.update_address,
                    args=[order_id, self.updated_address],
                    start_to_close_timeout=timedelta(seconds=2),
                    retry_policy=RetryPolicy(
                    initial_interval=timedelta(milliseconds=100),
                    backoff_coefficient=1.0,            # no growth
                    maximum_interval=timedelta(milliseconds=100),  # clamp at 1s
                    maximum_attempts=5  
                ),
                )
                workflow.logger.info(f"Workflow: Address updated successfully: {update_result}")
                # Update the order dict for the shipping workflow
                order["shipping_address"] = self.updated_address
            except Exception as e:
                workflow.logger.error(f"WORKFLOW STEP: Failed to update address: {e}")
                # Continue with original address if update fails
                workflow.logger.info("Continuing with original address")

        max_dispatch_attempts = 5
        for attempt in range(1, max_dispatch_attempts + 1):
            # Early exits
            if self.cancelled:
                workflow.logger.info(f"Order cancelled!")
                return "Order cancelled before shipping"

            try:
                workflow.logger.info(f"PARENT: starting ShippingWorkflow attempt {attempt}")
                await workflow.execute_child_workflow(
                    ShippingWorkflow.run,
                    args=[order, workflow.info().workflow_id],
                    id=f"shipping-{order_id}-attempt-{attempt}",  # unique per attempt
                    task_queue="shipping-tq",
                )
                workflow.logger.info(f"WORKFLOW STEP: Shipping completed for {order_id} on attempt {attempt}")
                self.current_step = "completed"
                return "Order workflow completed successfully"

            except ChildWorkflowError as e:
                cause = getattr(e, "cause", None)
                # If child raised dispatch error: we handle and retry
                if isinstance(cause, ApplicationError) and cause.type == "DispatchFailed":
                    workflow.logger.warning(f"PARENT: dispatch failed on attempt {attempt}: {cause.message}")
                    if attempt == max_dispatch_attempts:
                        workflow.logger.error("PARENT: max dispatch attempts reached")
                        return f"Order completed but shipping failed after {max_dispatch_attempts} attempts"

                    continue

                # Any other child failure: surface/abort
                workflow.logger.error(f"PARENT: child workflow failed: {e}")
                return f"Order completed but shipping failed: {e}"
        
        return "Order workflow completed successfully"

        