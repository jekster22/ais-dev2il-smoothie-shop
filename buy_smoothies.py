import time
import random
import httpx

# Available smoothie flavors
flavors = ["Strawberry", "Banana", "Mango", "Blueberry", "Spinach"]

def buy_smoothies():
    # Continuously order smoothies in a loop
    while True:
        # Pick a random flavor
        flavor = random.choice(flavors)
        try:
            print(f"I would like to have a {flavor} smoothie: ", end="")
            # Send order to the order service
            response = httpx.post("http://localhost:8000/order", json={"flavor": flavor})
            # Raise an error if the order failed
            response.raise_for_status()
            print(f"Thanks for the delicious {flavor} smoothie")
        except Exception as e:
            # Order failed (e.g., kitchen is busy or service is down)
            print(f"Failed to order a {flavor} smoothie: {e}")

# Run the script
if __name__ == "__main__":
    buy_smoothies()

