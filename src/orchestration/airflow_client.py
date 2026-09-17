"""Trigger Airflow DAG runs from the API via Airflow's stable REST API.

Used to hand slow, restart-sensitive work -- a new ticker's initial multi-year
price backfill -- to Airflow (a durable executor that persists and retries a run
across restarts) instead of a FastAPI BackgroundTask, which runs in the uvicorn
process and is lost silently if that process restarts mid-backfill.

Fail-soft by design: if Airflow isn't configured or is unreachable,
trigger_dag_run() returns False (never raises) so the caller can fall back to
the in-process path rather than turning a transient Airflow issue into a failed
ticker registration.
"""

from __future__ import annotations

from typing import Any

import requests

from src.core.config import settings
from src.core.logger import get_logger

logger = get_logger(__name__)

# The REST call only enqueues a DAG run (Airflow does the slow work async), so a
# short timeout is enough and keeps the request path from hanging on a wedged
# webserver -- we fall back to the in-process backfill instead.
_TIMEOUT_SECONDS = 5


def is_configured() -> bool:
    return bool(settings.airflow_api_url and settings.airflow_username and settings.airflow_password)


def trigger_dag_run(dag_id: str, conf: dict[str, Any]) -> bool:
    """Enqueue a run of `dag_id` with the given `conf`. Returns True iff Airflow
    accepted it. Never raises: any missing config, network error, or non-2xx
    response is logged and returns False so callers can degrade gracefully."""
    api_url = settings.airflow_api_url
    username = settings.airflow_username
    password = settings.airflow_password
    if not (api_url and username and password):
        logger.info(
            "Airflow REST not configured (AIRFLOW_API_URL/USERNAME/PASSWORD); skipping DAG trigger for %s",
            dag_id,
        )
        return False

    url = f"{api_url.rstrip('/')}/dags/{dag_id}/dagRuns"
    try:
        resp = requests.post(
            url,
            json={"conf": conf},
            auth=(username, password),
            timeout=_TIMEOUT_SECONDS,
        )
    except requests.RequestException as e:
        logger.warning("Airflow DAG trigger failed (network) for %s: %s", dag_id, e)
        return False

    if resp.status_code not in (200, 201):
        logger.warning(
            "Airflow DAG trigger for %s returned HTTP %s: %s", dag_id, resp.status_code, resp.text[:300]
        )
        return False

    logger.info("Triggered Airflow DAG %s with conf=%s", dag_id, conf)
    return True
