#!/usr/bin/env python3
"""
Simple test runner for Temporal Order Workflow tests.
"""
import asyncio
import sys
from datetime import timedelta

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

# Import your workflows and activities
from order_workflow import OrderWorkflow
from order_activities import OrderActivities
from shipping_workflow import ShippingWorkflow
from shipping_activities import ShippingActivities


async def test_basic_workflow():
    """Test basic workflow execution."""
    print("🧪 Testing basic workflow execution...")
    
    async with WorkflowEnvironment() as env:
        async with Worker(
            env.client,
            task_queue="test-tq",
            workflows=[OrderWorkflow],
            activities=[
                OrderActivities().receive_order,
                OrderActivities().validate_order,
                OrderActivities().charge_payment,
            ],
        ):
            # Start the workflow
            handle = await env.client.start_workflow(
                OrderWorkflow.run,
                "test-basic-123",
                "pay-basic-123",
                id="test-basic-workflow-123",
                task_queue="test-tq",
            )
            
            # Wait for completion
            result = await handle.result()
            
            print(f"✅ Workflow completed with result: {result}")
            return True


async def test_workflow_cancellation():
    """Test workflow cancellation."""
    print("🧪 Testing workflow cancellation...")
    
    async with WorkflowEnvironment() as env:
        async with Worker(
            env.client,
            task_queue="test-tq",
            workflows=[OrderWorkflow],
            activities=[
                OrderActivities().receive_order,
                OrderActivities().validate_order,
                OrderActivities().charge_payment,
            ],
        ):
            # Start the workflow
            handle = await env.client.start_workflow(
                OrderWorkflow.run,
                "test-cancel-123",
                "pay-cancel-123",
                id="test-cancel-workflow-123",
                task_queue="test-tq",
            )
            
            # Send cancel signal
            await handle.signal(OrderWorkflow.cancel_order_signal)
            
            # Wait for completion
            result = await handle.result()
            
            print(f"✅ Workflow cancelled with result: {result}")
            return True


async def test_workflow_status_query():
    """Test workflow status query."""
    print("🧪 Testing workflow status query...")
    
    async with WorkflowEnvironment() as env:
        async with Worker(
            env.client,
            task_queue="test-tq",
            workflows=[OrderWorkflow],
            activities=[
                OrderActivities().receive_order,
                OrderActivities().validate_order,
                OrderActivities().charge_payment,
            ],
        ):
            # Start the workflow
            handle = await env.client.start_workflow(
                OrderWorkflow.run,
                "test-query-123",
                "pay-query-123",
                id="test-query-workflow-123",
                task_queue="test-tq",
            )
            
            # Query workflow status
            status = await handle.query(OrderWorkflow.status)
            
            print(f"✅ Workflow status: {status}")
            return True


async def test_shipping_workflow():
    """Test shipping workflow."""
    print("🧪 Testing shipping workflow...")
    
    async with WorkflowEnvironment() as env:
        async with Worker(
            env.client,
            task_queue="test-shipping-tq",
            workflows=[ShippingWorkflow],
            activities=[
                ShippingActivities().prepare_package,
                ShippingActivities().dispatch_carrier,
            ],
        ):
            # Test order data
            order_data = {
                "order_id": "test-shipping-123",
                "state": "validated",
                "items": [{"sku": "ABC", "qty": 1}],
                "shipping_address": {
                    "street": "123 Test St",
                    "city": "Test City",
                    "state": "TS",
                    "zip": "12345"
                }
            }
            
            # Start the workflow
            handle = await env.client.start_workflow(
                ShippingWorkflow.run,
                order_data,
                id="test-shipping-workflow-123",
                task_queue="test-shipping-tq",
            )
            
            # Wait for completion
            result = await handle.result()
            
            print(f"✅ Shipping workflow completed with result: {result}")
            return True


async def test_activity_idempotency():
    """Test activity idempotency."""
    print("🧪 Testing activity idempotency...")
    
    from function_stubs import payment_charged
    
    # Mock order data
    order_data = {
        "order_id": "test-idempotency-123",
        "items": [{"sku": "ABC", "qty": 2}]
    }
    
    # First call
    result1 = await payment_charged(order_data, "pay-idempotency-123", None)
    print(f"First call result: {result1}")
    
    # Second call (should be idempotent)
    result2 = await payment_charged(order_data, "pay-idempotency-123", None)
    print(f"Second call result: {result2}")
    
    # Check idempotency
    if result1["payment_id"] == result2["payment_id"] and result1["status"] == result2["status"]:
        print("✅ Activity idempotency test passed")
        return True
    else:
        print("❌ Activity idempotency test failed")
        return False


async def run_all_tests():
    """Run all tests."""
    print("🚀 Starting Temporal Order Workflow Tests")
    print("=" * 50)
    
    tests = [
        ("Basic Workflow", test_basic_workflow),
        ("Workflow Cancellation", test_workflow_cancellation),
        ("Workflow Status Query", test_workflow_status_query),
        ("Shipping Workflow", test_shipping_workflow),
        ("Activity Idempotency", test_activity_idempotency),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            print(f"\n📋 Running: {test_name}")
            result = await test_func()
            if result:
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ {test_name} failed with error: {e}")
            failed += 1
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed!")
        return True
    else:
        print("💥 Some tests failed!")
        return False


if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)
