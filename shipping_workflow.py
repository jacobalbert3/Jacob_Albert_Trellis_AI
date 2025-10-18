from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from datetime import timedelta
from typing import Dict, Any

with workflow.unsafe.imports_passed_through():
    from shipping_activities import ShippingActivities

@workflow.defn
class ShippingWorkflow:
    @workflow.run
    async def run(self, order: Dict[str, Any], parent_wf_id: str) -> str:
        order_id = order.get("order_id", "unknown")
        info = workflow.info()
        workflow.logger.info(
            f"SHIPPING START: order={order_id} child_wf_id={info.workflow_id} "
            f"run_id={info.run_id} parent_wf_id={parent_wf_id}"
        )

        # 1) Preparation activity
        try:
            package_status = await workflow.execute_activity_method(
                ShippingActivities.prepare_package,
                args=[order],
                start_to_close_timeout=timedelta(seconds=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(milliseconds=100),
                    backoff_coefficient=1.0,            # no growth
                    maximum_interval=timedelta(milliseconds=100),  # clamp at 1s
                    maximum_attempts=5  
                ),
            )
            if package_status != "Package ready":
                raise RuntimeError(f"Package preparation failed: {package_status}")
            workflow.logger.info(f"SHIPPING: Package prepared for {order_id}")
        except Exception as e:
            reason = f"Package preparation failed: {type(e).__name__}: {e}"
            workflow.logger.error(f"SHIPPING: {reason}")
            # Signal parent using the ID we were passed
            parent = workflow.get_external_workflow_handle(parent_wf_id)
            await parent.signal("dispatch_failed", reason)
            await workflow.sleep(timedelta(milliseconds=1))
            # Typed, non-retryable failure so the PARENT loop takes over
            raise ApplicationError("PACKAGE_PREP_FAILED", type="DispatchFailed", non_retryable=True)

        # 2) Dispatch (no internal retries; parent decides)
        workflow.logger.info(f"SHIPPING: Dispatch for {order_id}")
        try:
            dispatch_status = await workflow.execute_activity_method(
                ShippingActivities.dispatch_carrier,
                args=[order],
                start_to_close_timeout=timedelta(seconds=2),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(milliseconds=100),
                    backoff_coefficient=1.0,            # no growth
                    maximum_interval=timedelta(milliseconds=100),  # clamp at 1s
                    maximum_attempts=5  
                ),
            )
            if dispatch_status != "Dispatched":
                raise RuntimeError(f"Dispatch failed: {dispatch_status}")
            workflow.logger.info(f"SHIPPING: Carrier dispatched for {order_id}")
        except Exception as e:
            reason = f"{type(e).__name__}: {e}"
            # Signal parent using the ID we were passed
            parent = workflow.get_external_workflow_handle(parent_wf_id)
            await parent.signal("dispatch_failed", reason)
            await workflow.sleep(timedelta(milliseconds=1))
            raise ApplicationError("DISPATCH_FAILED", type="DispatchFailed", non_retryable=True)

        return "Shipping complete"
