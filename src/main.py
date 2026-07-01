from fastapi import FastAPI

from src.api.routers import fundamentals, instruments, prices

app = FastAPI(title="Finance Data Platform API")

app.include_router(instruments.router)
app.include_router(prices.router)
app.include_router(fundamentals.router)
