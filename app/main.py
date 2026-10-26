import logging
import os
import socket
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel

from .mqtt_async import AsyncMQTT
from .pipeline import Pipeline
from .settings import Settings


class RunStart(BaseModel):
    run_id: str
    batch_size: Optional[int] = None
    max_delay_s: Optional[float] = None
    policy: Optional[str] = None
    max_inflight: Optional[int] = None
    resume_ratio: Optional[float] = None
    storage: Optional[str] = None
    log_batches: Optional[bool] = None
    validate_schema: Optional[bool] = None
    qos: Optional[int] = None
    discard: bool = False
    reset_db: bool = True


class RunStop(BaseModel):
    t0: Optional[float] = None
    t1: Optional[float] = None


class Idle(BaseModel):
    timeout_s: float = 60.0


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()
    pipe = Pipeline(settings)
    pipe.mqtt = AsyncMQTT(settings.mqtt_host, settings.mqtt_port, f"ingest-{socket.gethostname()}",
                          topic=settings.topic, qos=settings.runtime.qos, on_message=pipe.on_message)
    await pipe.start()
    app.state.pipe = pipe
    yield
    await pipe.stop()


app = FastAPI(title="ingest-service (Lab 5)", lifespan=lifespan)


def _pipe() -> Pipeline:
    return app.state.pipe


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(_pipe().registry), media_type=CONTENT_TYPE_LATEST)


@app.get("/healthz")
async def healthz():
    p = _pipe()
    ok = p.mqtt_connected()
    body = {"status": "ok" if ok else "no-broker", "mqtt_connected": ok, "inflight": p.inflight,
            "queue_depth": p.queue_depth, "paused": p.paused, "run_id": p.run_id}
    if not ok:
        raise HTTPException(status_code=503, detail=body)
    return body


@app.get("/config")
async def config():
    return _pipe().cfg.as_dict()


@app.post("/run/start")
async def run_start(req: RunStart):
    p = _pipe()
    over = req.model_dump(exclude={"run_id", "discard", "reset_db"})
    try:
        return await p.start_run(req.run_id, over, discard=req.discard, reset_db=req.reset_db)
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=422, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))


@app.post("/run/stop")
async def run_stop(req: RunStop):
    return await _pipe().stop_run(req.t0, req.t1)


@app.post("/run/idle")
async def run_idle(req: Idle):
    p = _pipe()
    ok = await p.wait_idle(req.timeout_s)
    return {"idle": ok, "inflight": p.inflight}
