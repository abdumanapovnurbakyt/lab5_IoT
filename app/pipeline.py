import array
import asyncio
import collections
import json
import logging
import time
from bisect import bisect_left, bisect_right
from concurrent.futures import ThreadPoolExecutor

from jsonschema import Draft202012Validator

from .metrics import Metrics, make_registry
from .settings import RuntimeConfig
from .stats import summarize
from .storage import NullStorage, SqliteStorage

log = logging.getLogger("ingest")
perf = time.perf_counter

TYPES = ("temp-hum-A", "temp-hum-B", "temp-hum-C")
UNKNOWN = ("unknown", "unknown")
SLICE = 100         


def build_device_registry(n=10000):
    return {f"dev-{i:05d}": (f"site-{i % 8}", TYPES[i % 3]) for i in range(n)}


class RunState:

    def __init__(self, run_id, m):
        self.run_id = run_id
        self.t0 = time.time()
        self.base = {"received": m.received, "invalid": dict(m.invalid), "stored": m.stored,
                     "dropped": dict(m.dropped), "flush": dict(m.flush), "mqtt_read_s": m.mqtt_read_s,
                     "process_s": m.process_s, "storage_s": m.storage_s, "pause_s": m.pause_s, "pauses": m.pauses}
        self.commit_t = array.array("d")
        self.proc = array.array("d")
        self.e2e = array.array("d")
        self.batch_sizes = array.array("I")

    def add(self, t_commit, proc, e2e, n):
        self.commit_t.extend([t_commit] * n)
        self.proc.extend(proc)
        self.e2e.extend(e2e)
        self.batch_sizes.append(n)

    def window(self, t0, t1):
        lo = 0 if t0 is None else bisect_left(self.commit_t, t0)
        hi = len(self.commit_t) if t1 is None else bisect_right(self.commit_t, t1)
        return lo, hi


class Pipeline:
    def __init__(self, settings, mqtt=None):
        self.settings = settings
        self.cfg = RuntimeConfig(**settings.runtime.as_dict()).validate()
        self.m = Metrics()
        self.raw = collections.deque()        
        self.q = collections.deque()           
        self.inflight = 0
        self.paused = False
        self._paused_since = None
        self.mqtt = mqtt
        self.run_id = "idle"
        self.run = RunState(self.run_id, self.m)
        with open(settings.schema_path, encoding="utf-8") as f:
            schema = json.load(f)
        Draft202012Validator.check_schema(schema)
        self.validator = Draft202012Validator(schema)
        self.meta = build_device_registry()
        self._sqlite = None
        self._null = NullStorage()
        self._pool = ThreadPoolExecutor(1, thread_name_prefix="storage")
        self._ev_raw = asyncio.Event()
        self._ev_nonempty = asyncio.Event()
        self._ev_full = asyncio.Event()
        self._tasks = []
        self._batch_no = 0
        self._agg = [0, 0, 0, 0.0]        
        self.registry = make_registry(self)

    async def start(self):
        self._sqlite = SqliteStorage(self.settings.db_path, self.settings.sqlite_sync)
        self._tasks = [asyncio.create_task(self._processor(), name="processor"),
                       asyncio.create_task(self._writer(), name="writer"),
                       asyncio.create_task(self._monitor(), name="monitor")]
        if self.mqtt is not None:
            await self.mqtt.start()

    async def stop(self):
        if self.mqtt is not None:
            await self.mqtt.stop()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._pool.shutdown(wait=True)
        if self._sqlite is not None:
            self._sqlite.close()

    @property
    def storage(self):
        return self._null if self.cfg.storage == "null" else self._sqlite

    def mqtt_connected(self):
        return bool(self.mqtt and self.mqtt.connected)

    def sync_read(self):
        if self.mqtt is not None:
            self.m.mqtt_read_s = self.mqtt.read_seconds

    def current_pause_s(self):
        return (time.monotonic() - self._paused_since) if (self.paused and self._paused_since) else 0.0

    @property
    def queue_depth(self):
        return len(self.raw) + len(self.q)

    def on_message(self, client, userdata, msg):
        """Callback paho: вызывается внутри цикла событий для каждого PUBLISH."""
        self.accept(msg.payload)

    def accept(self, payload):
        m, cfg = self.m, self.cfg
        m.received += 1
        if cfg.policy == "shed" and self.inflight >= cfg.max_inflight:
            m.dropped["shed"] += 1
            return
        self.raw.append((time.time(), payload))
        self.inflight += 1
        if len(self.raw) == 1:
            self._ev_raw.set()
        if cfg.policy == "pause" and self.inflight >= cfg.max_inflight:
            self._pause()

    def _pause(self):
        if not self.paused and self.mqtt is not None and self.mqtt.pause():
            self.paused = True
            self._paused_since = time.monotonic()
            self.m.pauses += 1

    def _resume(self):
        if self.paused:
            self.m.pause_s += self.current_pause_s()
            self.paused = False
            self._paused_since = None
            if self.mqtt is not None:
                self.mqtt.resume()

    def _maybe_resume(self):
        cfg = self.cfg
        if self.paused and self.inflight <= cfg.max_inflight * cfg.resume_ratio:
            self._resume()

    async def _processor(self):
        raw = self.raw
        while True:
            if not raw:
                self._ev_raw.clear()
                await self._ev_raw.wait()
                continue
            t = perf()
            n = 0
            while raw and n < SLICE:
                t_recv, payload = raw.popleft()
                self._process(t_recv, payload)
                n += 1
            self.m.process_s += perf() - t
            self._maybe_resume()
            await asyncio.sleep(0)                 

    def _process(self, t_recv, payload):
        m, cfg = self.m, self.cfg
        try:
            obj = json.loads(payload)
        except ValueError:
            m.invalid["json"] += 1
            self.inflight -= 1
            return
        if cfg.validate_schema and not self.validator.is_valid(obj):
            m.invalid["schema"] += 1
            self.inflight -= 1
            return
        try:
            dev = obj["device_id"]
            site, dtype = self.meta.get(dev, UNKNOWN)
            rec = (dev, obj["seq"], obj["ts"], t_recv, obj["temperature"], obj["humidity"], obj["battery_mv"],
                   site, dtype, self.run_id)
        except (KeyError, TypeError):             
            m.invalid["schema"] += 1
            self.inflight -= 1
            return
        q = self.q
        q.append(rec)
        m.processed += 1
        n = len(q)
        if n == 1:
            self._ev_nonempty.set()
        if n >= cfg.batch_size:
            self._ev_full.set()

    async def _writer(self):
        while True:
            if not self.q:
                self._ev_nonempty.clear()
                await self._ev_nonempty.wait()
                continue
            cfg = self.cfg
            reason = "size"
            if len(self.q) < cfg.batch_size:
                deadline = self.q[0][3] + cfg.max_delay_s     
                while len(self.q) < cfg.batch_size:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        reason = "timeout"
                        break
                    self._ev_full.clear()
                    try:
                        await asyncio.wait_for(self._ev_full.wait(), remaining)
                    except asyncio.TimeoutError:
                        reason = "timeout"
                        break
            n = min(len(self.q), cfg.batch_size)
            if n == 0:                                           
                continue
            batch = [self.q.popleft() for _ in range(n)]
            await self._flush(batch, reason)

    async def _flush(self, batch, reason):
        m, cfg = self.m, self.cfg
        n = len(batch)
        t_start = time.time()
        wait_oldest = t_start - batch[0][3]
        try:
            if cfg.storage == "null":
                write_s = 0.0
                await asyncio.sleep(0)                          
            else:
                write_s = await asyncio.get_running_loop().run_in_executor(self._pool, self._sqlite.write, batch)
        except Exception:                                     
            log.exception("ошибка записи пакета из %d сообщений", n)
            m.dropped["storage_error"] += n
            self.inflight -= n
            self._maybe_resume()
            return
        t_commit = time.time()
        proc = [t_commit - r[3] for r in batch]
        e2e = [t_commit - r[2] for r in batch]
        m.storage_s += write_s
        m.write.observe(write_s)
        m.proc.observe_many(proc)
        m.e2e.observe_many(e2e)
        m.batch.observe(n)
        m.flush[reason] += 1
        m.stored += n
        self.run.add(t_commit, proc, e2e, n)
        self.inflight -= n
        self._batch_no += 1
        if cfg.log_batches:
            log.info("пакет #%d: размер=%d (из %d) причина=%s ждал=%.3f с запись=%.1f мс inflight=%d",
                     self._batch_no, n, cfg.batch_size, reason, wait_oldest, write_s * 1000, self.inflight)
        else:
            a = self._agg
            a[0] += 1
            a[1] += n
            a[2] = max(a[2], n)
            a[3] += write_s
        self._maybe_resume()

    async def _monitor(self):
        loop = asyncio.get_running_loop()
        last_log = loop.time()
        while True:
            t = loop.time()
            await asyncio.sleep(0.05)
            lag = loop.time() - t - 0.05
            if lag > self.m.lag_max:
                self.m.lag_max = lag
            if loop.time() - last_log >= 1.0:
                last_log = loop.time()
                a = self._agg
                if a[0]:
                    log.info("сводка за 1 с: пакетов=%d сообщений=%d средний размер=%.1f макс=%d запись=%.1f мс/пакет "
                             "inflight=%d", a[0], a[1], a[1] / a[0], a[2], a[3] / a[0] * 1000, self.inflight)
                    self._agg = [0, 0, 0, 0.0]

    async def wait_idle(self, timeout):
        t = time.monotonic()
        while time.monotonic() - t < timeout:
            if self.inflight == 0 and not self.raw and not self.q:
                return True
            await asyncio.sleep(0.05)
        return self.inflight == 0 and not self.raw and not self.q

    async def discard(self):
        n = len(self.raw) + len(self.q)
        self.raw.clear()
        self.q.clear()
        self.inflight -= n
        self.m.dropped["discarded"] += n
        self._resume()
        if self.mqtt is not None:
            await self.mqtt.restart()
        return n

    async def start_run(self, run_id, overrides, discard=False, reset_db=True):
        discarded = await self.discard() if discard else 0
        if discard:
            await self.wait_idle(10.0)                          
        if self.inflight != 0 or self.raw or self.q:
            raise RuntimeError(f"конвейер не пуст (inflight={self.inflight}); дождитесь опустошения или discard=true")
        new = {**self.cfg.as_dict(), **{k: v for k, v in overrides.items() if v is not None}}
        self.cfg = RuntimeConfig(**new).validate()
        if self.mqtt is not None:
            self.mqtt.set_qos(self.cfg.qos)
        self.run_id = run_id
        if reset_db and self._sqlite is not None:
            await asyncio.get_running_loop().run_in_executor(self._pool, self._sqlite.clear)
        self.m.lag_max = 0.0
        self.sync_read()
        self.run = RunState(run_id, self.m)
        return {"t0": self.run.t0, "discarded": discarded, "config": self.cfg.as_dict()}

    async def stop_run(self, t0=None, t1=None):
        self.sync_read()
        m, r = self.m, self.run
        b = r.base
        lo, hi = r.window(t0, t1)
        rows = distinct = 0
        if self.cfg.storage == "sqlite" and self._sqlite is not None:
            rows, distinct = await asyncio.get_running_loop().run_in_executor(
                self._pool, self._sqlite.verify, r.run_id)
        bs = r.batch_sizes
        return {
            "run_id": r.run_id, "t0": r.t0, "t_stop": time.time(), "config": self.cfg.as_dict(),
            "received": m.received - b["received"],
            "invalid": {k: m.invalid[k] - b["invalid"][k] for k in m.invalid},
            "stored": m.stored - b["stored"],
            "dropped": {k: m.dropped[k] - b["dropped"][k] for k in m.dropped},
            "inflight_end": self.inflight, "queue_end": self.queue_depth,
            "flush": {k: m.flush[k] - b["flush"][k] for k in m.flush},
            "batches": len(bs), "batch_mean": (sum(bs) / len(bs)) if len(bs) else None,
            "batch_min": min(bs) if len(bs) else None, "batch_max": max(bs) if len(bs) else None,
            "db": {"rows": rows, "distinct": distinct, "duplicates": rows - distinct},
            "latency_window": {"proc": summarize(r.proc[lo:hi]), "e2e": summarize(r.e2e[lo:hi])},
            "latency_all": {"proc": summarize(r.proc), "e2e": summarize(r.e2e)},
            "busy_s": {"mqtt_read": m.mqtt_read_s - b["mqtt_read_s"], "process": m.process_s - b["process_s"],
                       "storage": m.storage_s - b["storage_s"]},
            "pause": {"count": m.pauses - b["pauses"], "seconds": (m.pause_s - b["pause_s"]) + self.current_pause_s()},
        }
