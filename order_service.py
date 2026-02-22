import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Create the FastAPI application
app = FastAPI(title="Order Service")

# Data model: Defines what an order looks like
class Order(BaseModel):
    flavor: str

# Endpoint: Receives customer orders
@app.post("/order")
async def create_order(order: Order):
    # Create an HTTP client to communicate with the kitchen service
    async with httpx.AsyncClient() as client:
        try:
            # Send the order to the kitchen service
            response = await client.post(
                "http://localhost:8001/prepare",
                json={"flavor": order.flavor}
            )
            # Raise an error if the kitchen returned an error status code
            response.raise_for_status()
            return {"status": "completed", "kitchen_response": response.json()}
        except httpx.HTTPStatusError as e:
            # Kitchen returned an error (e.g., 503 if all cooks are busy)
            raise HTTPException(status_code=e.response.status_code, detail="Kitchen failed to process order")
        except httpx.RequestError:
            # Could not connect to the kitchen service
            raise HTTPException(status_code=503, detail="Kitchen unavailable")
