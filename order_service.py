import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)
# Create the FastAPI application
app = FastAPI(title="Order Service")

# Data model: Defines what an order looks like
class Order(BaseModel):
    flavor: str

# Endpoint: Receives customer orders
@app.post("/order")
async def create_order(order: Order):
    # Create an HTTP client to communicate with the kitchen service
    logger.info(f"Receive order with flavour {order.flavor}")
    logger.debug(f"Order-json: {order}")
    async with httpx.AsyncClient() as client:
        try:
            logger.info(f"Sending order with flavour {order.flavor}")
            # Send the order to the kitchen service
            response = await client.post(
                "http://localhost:8001/prepare",
                json={"flavor": order.flavor}
            )
            # Raise an error if the kitchen returned an error status code
            response.raise_for_status()
            logger.info(f"Completed order with flavour {order.flavor}")
            return {"status": "completed", "kitchen_response": response.json()}
        except httpx.HTTPStatusError as e:
            # Kitchen returned an error (e.g., 503 if all cooks are busy)
            logger.error(f"Kitchen error - Status: {e.response.status_code}, Response: {e.response.text}")
            raise HTTPException(status_code=e.response.status_code, detail="Kitchen failed to process order")
        except httpx.RequestError as e:
            logger.error(f"Request error - Kitchen service is unavailable:")
            # Could not connect to the kitchen service
            raise HTTPException(status_code=503, detail="Kitchen unavailable")