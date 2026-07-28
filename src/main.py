from fastapi import FastAPI

from src.api.routes import fundamentals, health, instruments, prices

app = FastAPI(title="Finance Data Platform API")

app.include_router(health.router)
app.include_router(instruments.router)
app.include_router(prices.router)
app.include_router(fundamentals.router)


if __name__ == "__main__":
    ...
