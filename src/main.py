from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from src.api.routes import fundamentals, health, instruments, macro, prices
from src.core.config import settings
from src.core.logger import get_logger

logger = get_logger(__name__)

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend" / "dist"

app = FastAPI(title="Finance Data Platform API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=False,  # must be False when allow_origins can be "*"
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catches anything that isn't already an HTTPException/validation error
    (those get FastAPI's own handlers, which run first) -- a raw Postgres/S3
    error no longer leaks a stack trace to the client, just a generic 500.
    The real detail still goes to the logs.
    """
    logger.error("Unhandled exception on %s %s: %s", request.method, request.url.path, exc)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


app.include_router(health.router)
app.include_router(instruments.router)
app.include_router(prices.router)
app.include_router(fundamentals.router)
app.include_router(macro.router)

# Internal admin UI (add/toggle tickers, browse the Gold tables) -- a React +
# Vite app, built (`npm run build` in frontend/) to frontend/dist and served
# as static files here, same-origin so it hits the API routes above without
# any CORS involved. Mounted last, at /app rather than "/", so it can never
# shadow an API route. Guarded on the dist/ dir existing so the API still
# boots if the frontend hasn't been built (e.g. a fresh checkout).
if FRONTEND_DIR.is_dir():
    app.mount("/app", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
else:
    logger.warning("frontend/dist not found -- run `npm run build` in frontend/ to serve the UI at /app")


if __name__ == "__main__":
    ...
