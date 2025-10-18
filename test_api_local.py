import pytest
import httpx
import time
import json


class TestAPILocal:
    """Test suite for the Temporal Order Workflow API."""
    
    def test_health_check(self):
        """Test that the API health check endpoint returns the expected response."""
        with httpx.Client() as client:
            response = client.get("http://localhost:8000/")
            
            assert response.status_code == 200
            data = response.json()
            assert data["message"] == "Temporal Order Workflow API is running"
            print("Health check passed")

    def test_complete_order_workflow(self):
        """Test complete order workflow by calling API endpoints."""
        test_order_id = "test-order-123"
        test_payment_id = "test-payment-456"
        
        with httpx.Client() as client:
            # Step 1: Start the order workflow
            print(f"Starting order workflow for {test_order_id}")
            start_response = client.post(
                f"http://localhost:8000/orders/{test_order_id}/start",
                json={"payment_id": test_payment_id}
            )
            
            assert start_response.status_code == 200
            start_data = start_response.json()
            assert start_data["order_id"] == test_order_id
            assert start_data["status"] == "started"
            workflow_id = start_data["workflow_id"]
            print(f"Workflow started with ID: {workflow_id}")
            
            # Give the workflow moment to start
            time.sleep(1)
            
            # Step 2: Wait for workflow to complete (poll status)
            print("Waiting for workflow to complete...")
            max_wait_time = 30
            start_time = time.time()
            
            while time.time() - start_time < max_wait_time:
                status_response = client.get(f"http://localhost:8000/orders/{test_order_id}/status")
                
                if status_response.status_code == 404:
                    print("Order not found yet, waiting for workflow to create it...")
                    time.sleep(2)
                    continue
                elif status_response.status_code != 200:
                    print(f"Status check failed with {status_response.status_code}")
                    print(f"Response: {status_response.text}")
                    break
                
                # If we get here, status_response.status_code == 200
                
                status_data = status_response.json()
                
                status = status_data['status']
                current_step = status_data['current_step']

                print(f"Status: {status}")
                print(f"Current step: {current_step}")

                # Check if workflow is completed
                if status == "COMPLETED":
                    print("Workflow completed successfully!")
                    break
                elif status == "FAILED":
                    print(f"Workflow failed at step: {current_step}")
                    assert True, f"Workflow failed with status: {status_data}"
                elif status == "CANCELED":
                    print(f"Workflow was canceled at step: {current_step}")
                    assert True, f"Workflow was canceled: {status_data}"
                
                time.sleep(2)  # Wait 2 seconds before next check
            else:
                assert False, f"Workflow did not complete within {max_wait_time} seconds"
            
            assert True


    def test_get_order_events(self):
        """Test getting order events."""
        test_order_id = "test-order-123"
        
        with httpx.Client() as client:
            response = client.get(f"http://localhost:8000/orders/{test_order_id}/events")
            
            assert response.status_code == 200
            events_data = response.json()
            assert events_data["order_id"] == test_order_id
            assert "events" in events_data
            assert len(events_data["events"]) > 0
            
            print("Order events retrieved successfully")
            print(f"Found {len(events_data['events'])} events")
            
            # Print first few events
            for i, event in enumerate(events_data["events"][:3]):
                print(f"  {i+1}. {event['event_type']} at {event['timestamp']}")

    def test_cancel_order(self):
        #create an order
        test_order_id = "test-cancel-order"
        test_payment_id = "test-payment-456"

        with httpx.Client() as client:
            # Step 1: Start the order workflow
            print(f"Starting order workflow for {test_order_id}")
            start_response = client.post(
                f"http://localhost:8000/orders/{test_order_id}/start",
                json={"payment_id": test_payment_id}
            )
            
            #step 2 - cancel the order
            print(f"Canceling order workflow for {test_order_id}")
            cancel_response = client.post(
                f"http://localhost:8000/orders/{test_order_id}/signals/cancel"
            )
            
            assert cancel_response.status_code == 200
            cancel_data = cancel_response.json()

            #step 3 - check the status of the order (every 2 seconds for 30 seconds)
            print(f"Waiting for order to be canceled...")
            max_wait_time = 45
            start_time = time.time()
            
            while time.time() - start_time < max_wait_time:
                status_response = client.get(f"http://localhost:8000/orders/{test_order_id}/status")
            
                
                if status_response.status_code == 404:
                    print("Order not found yet, waiting for workflow to create it...")
                    time.sleep(2)
                    continue
                elif status_response.status_code != 200:
                    print(f"Status check failed with {status_response.status_code}")
                    print(f"Response: {status_response.text}")
                    break
                
                status_data = status_response.json()
                status = status_data['status']
                current_step = status_data['current_step']

                print(f"Status: {status}")
                print(f"Current step: {current_step}")
                
                if status == "COMPLETED":
                    print("Order completed, checking if it was canceled...")
                    break
                elif status == "FAILED":
                    print(f"Order failed at step: {current_step}")
                    assert False, f"Order failed with status: {status_data}"
                elif status == "TIMED_OUT":
                    print(f"Order timed out at step: {current_step}")
                    assert False, f"Order timed out: {status_data}"
                time.sleep(2)  # Wait 2 seconds before next check
            else:
                assert False, f"Order did not cancel within {max_wait_time} seconds"
            
            #step 4 - check if the order was canceled
            events_response = client.get(f"http://localhost:8000/orders/{test_order_id}/events")
            events_data = events_response.json()
            print(events_data["events"][-1]['event_type'])
            print("Order was canceled successfully!")
            assert True
    
    def test_update_order_address(self):
        """Test updating the order address."""
        test_order_id = "test-update-order"
        test_payment_id = "test-payment-456"

        with httpx.Client() as client:
            # Step 1: Start the order workflow
            print(f" Starting order workflow for {test_order_id}")
            start_response = client.post(
                f"http://localhost:8000/orders/{test_order_id}/start",
                json={"payment_id": test_payment_id}
            )
            
            #step 2 - update the order address
            print(f"Updating order address for {test_order_id}")
            update_response = client.post(
                f"http://localhost:8000/orders/{test_order_id}/signals/update-address",
                json={"street": "updated address street", "city": "Anytown", "state": "CA", "zip": "12345"}
            )
            
            assert update_response.status_code == 200
            update_data = update_response.json()
            
            #step 3 - check the status of the order (every 2 seconds for 30 seconds)
            print(f"Waiting for order to be updated...")
            max_wait_time = 45
            start_time = time.time()
            
            while time.time() - start_time < max_wait_time:
                status_response = client.get(f"http://localhost:8000/orders/{test_order_id}/status")
                
                if status_response.status_code == 404:
                    print("Order not found yet, waiting for workflow to create it...")
                    time.sleep(2)
                    continue
                elif status_response.status_code != 200:
                    print(f"Status check failed with {status_response.status_code}")
                    print(f"Response: {status_response.text}")
                    break
                
                status_data = status_response.json()
                status = status_data['status']
                current_step = status_data['current_step']

                print(f"Status: {status}")
                print(f"Current step: {current_step}")
                
                if status == "COMPLETED":
                    print("Order completed, checking if it was updated...")
                    break
                elif status == "FAILED":
                    print(f"Order failed at step: {current_step}")
                    assert False, f"Order failed with status: {status_data}"
                elif status == "TIMED_OUT":
                    print(f"Order timed out at step: {current_step}")
                    assert False, f"Order timed out: {status_data}"
                time.sleep(2)  # Wait 2 seconds before next check
            else:
                assert False, f"Order did not update within {max_wait_time} seconds"
            
            #step 4 - check if the order was updated
            events_response = client.get(f"http://localhost:8000/orders/{test_order_id}/events")
            events_data = events_response.json()
            print(events_data["events"][-1]['event_type'])
            print("Order was updated successfully!")
            assert True
if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
