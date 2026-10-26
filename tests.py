import asyncio
import json
import os
import random
import sqlite3
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from prometheus_client import generate_latest  # noqa: E402

from app.metrics import Hist  # noqa: E402
from app.pipeline import Pipeline  # noqa: E402
from app.settings import RuntimeConfig, Settings  # noqa: E402
from app.stats import hist_quantile, ols_slope, percentile, summarize  # noqa: E402
from tools.simulator import make_payload  # noqa: E402

OK = []


def check(name, cond, info=""):
    assert cond, f"НЕ ПРОЙДЕНО: {name} {info}"
    OK.append(name)
    print(f"  ок  {name} {info}")


class FakeMQTT:
    connected = True
    read_seconds = 0.0

    def __init__(self):
        self.paused = False
        self.pauses = self.resumes = 0

    def pause(self):
        self.paused = True
        self.pauses += 1
        return True

    def resume(self):
        self.paused = False
        self.resumes += 1
        return True

    def set_qos(self, qos):
        pass

    async def start(self):
        pass

    async def stop(self):
        pass

    async def restart(self):
        pass


def payload(i, seq=0, **over):
    msg = {"device_id": f"dev-{i:05d}", "seq": seq, "ts": time.time(), "temperature": 21.5, "humidity": 40.0,
           "battery_mv": 3700}
    msg.update(over)
    return json.dumps(msg).encode()


async def make_pipeline(tmp, **cfg):
    rt = RuntimeConfig(**{"log_batches": False, **cfg}).validate()
    st = Settings("none", 0, "t", os.path.join(tmp, "t.db"), os.path.join(HERE, "schemas", "telemetry.schema.json"),
                  "NORMAL", rt)
    fake = FakeMQTT()
    p = Pipeline(st, mqtt=fake)
    await p.start()
    return p, fake


def test_stats():
    v = list(range(1, 101))
    check("перцентили как в numpy: p50=50,5 p95=95,05 p99=99,01",
          abs(percentile(v, 50) - 50.5) < 1e-9 and abs(percentile(v, 95) - 95.05) < 1e-9
          and abs(percentile(v, 99) - 99.01) < 1e-9)
    s = summarize([0.1, 0.2, 0.3])
    check("summarize: n, среднее, максимум", s["n"] == 3 and abs(s["mean"] - 0.2) < 1e-12 and s["max"] == 0.3)
    slope, r2 = ols_slope([0, 1, 2, 3, 4], [10, 12, 14, 16, 18])
    check("наклон МНК = 2, R^2 = 1", abs(slope - 2) < 1e-9 and abs(r2 - 1) < 1e-9)
    h = Hist((0.1, 0.5, 1.0))
    h.observe_many([0.05, 0.2, 0.3, 0.7, 5.0])
    check("гистограмма: накопительные корзины", h.buckets() == [("0.1", 1), ("0.5", 3), ("1.0", 4), ("+Inf", 5)])
    b = [(0.1, 10), (0.5, 90), (1.0, 100), (float("inf"), 100)]
    check("перцентиль по гистограмме (как histogram_quantile)", abs(hist_quantile(b, 50) - 0.3) < 1e-9)


def test_schema_and_simulator():
    import jsonschema
    schema = json.load(open(os.path.join(HERE, "schemas", "telemetry.schema.json"), encoding="utf-8"))
    v = jsonschema.Draft202012Validator(schema)
    good = json.loads(payload(42))
    check("схема: корректное сообщение проходит", v.is_valid(good))
    bad = [dict(good, humidity=140.0), {k: x for k, x in good.items() if k != "battery_mv"}, dict(good, extra=1),
           dict(good, device_id="sensor-1"), dict(good, seq=-1), dict(good, temperature="warm")]
    check("схема: 6 видов некорректных сообщений отклоняются", all(not v.is_valid(b) for b in bad))
    rng = random.Random(7)
    n_bad = 0
    for i in range(4000):
        raw, ok = make_payload(i % 500, i, time.time(), rng, 0.05)
        valid = v.is_valid(json.loads(raw))
        assert valid == ok
        n_bad += not ok
    check("симулятор: невалидные сообщения действительно нарушают схему", True, f"(доля {n_bad / 4000:.3f} при заданной 0,05)")
    check("симулятор: доля невалидных близка к заданной", 0.03 < n_bad / 4000 < 0.07)


async def _batching(tmp):
    p, _ = await make_pipeline(tmp, batch_size=50, max_delay_s=0.3)
    await p.start_run("t-size", {})
    for i in range(120):
        p.accept(payload(i, i))
    await asyncio.sleep(1.0)
    sizes = list(p.run.batch_sizes)
    check("батчинг: 120 сообщений при batch_size=50 -> пакеты 50, 50, 20", sizes == [50, 50, 20], f"(получили {sizes})")
    check("батчинг: два пакета по размеру, один по таймеру", p.m.flush == {"size": 2, "timeout": 1}, f"({p.m.flush})")
    await p.stop()

    p, _ = await make_pipeline(tmp, batch_size=500, max_delay_s=0.3)
    await p.start_run("t-time", {})
    t = time.time()
    for i in range(10):
        p.accept(payload(i, i))
    await asyncio.sleep(0.7)
    st = await p.stop_run()
    check("батчинг: 10 сообщений при batch_size=500 сбрасываются по max_delay", st["batch_max"] == 10
          and st["flush"]["timeout"] == 1)
    lat = st["latency_all"]["proc"]
    check("батчинг: задержка не меньше max_delay (сообщение ждало пакет)", lat["p50"] >= 0.29, f"(p50 = {lat['p50']:.3f} с)")
    await p.stop()

    p, _ = await make_pipeline(tmp, batch_size=1, max_delay_s=0.3)
    await p.start_run("t-one", {})
    for i in range(7):
        p.accept(payload(i, i))
    await asyncio.sleep(0.5)
    check("батчинг: batch_size=1 -> каждое сообщение отдельным пакетом", list(p.run.batch_sizes) == [1] * 7)
    await p.stop()


def test_batching():
    with tempfile.TemporaryDirectory() as tmp:
        asyncio.run(_batching(tmp))


async def _validation(tmp):
    p, _ = await make_pipeline(tmp, batch_size=20, max_delay_s=0.2)
    await p.start_run("t-val", {})
    p.accept(b"{not json")
    p.accept(payload(1, 1, humidity=999))
    p.accept(json.dumps({"device_id": "dev-00002"}).encode())
    for i in range(10):
        p.accept(payload(42, i))
    await asyncio.sleep(0.6)
    st = await p.stop_run()
    check("валидация: сломанный JSON и нарушения схемы отклонены и посчитаны", st["invalid"] == {"json": 1, "schema": 2},
          f"({st['invalid']})")
    check("валидация: записаны только корректные (10 из 13)", st["stored"] == 10 and st["db"]["rows"] == 10)
    check("учёт: received = stored + invalid", st["received"] == st["stored"] + sum(st["invalid"].values()))
    check("после обработки inflight = 0", p.inflight == 0)
    row = sqlite3.connect(os.path.join(tmp, "t.db")).execute(
        "SELECT site, device_type, run_id FROM telemetry WHERE device_id='dev-00042' LIMIT 1").fetchone()
    check("обогащение: площадка и тип датчика добавлены из реестра", row == ("site-2", "temp-hum-A", "t-val"), f"({row})")
    for i in range(5):
        p.accept(payload(7, 100))                      # повторы той же пары device_id+seq
    await asyncio.sleep(0.5)
    st2 = await p.stop_run()
    check("проверка дубликатов: повторы одного (device_id, seq) находятся", st2["db"]["duplicates"] == 4,
          f"({st2['db']})")
    await p.stop()


def test_validation_enrichment():
    with tempfile.TemporaryDirectory() as tmp:
        asyncio.run(_validation(tmp))


async def _policies(tmp):
    p, fake = await make_pipeline(tmp, batch_size=1000, max_delay_s=0.3, policy="shed", max_inflight=30)
    await p.start_run("t-shed", {})
    for i in range(100):
        p.accept(payload(i, i))
    await asyncio.sleep(0.8)
    st = await p.stop_run()
    check("shed: сверх max_inflight сообщения отбрасываются и считаются", st["dropped"]["shed"] == 70
          and st["stored"] == 30, f"(отброшено {st['dropped']['shed']}, записано {st['stored']})")
    check("shed: чтение из MQTT не останавливается", fake.pauses == 0)
    await p.stop()

    p, fake = await make_pipeline(tmp, batch_size=1000, max_delay_s=0.3, policy="pause", max_inflight=20,
                                  resume_ratio=0.5)
    await p.start_run("t-pause", {})
    for i in range(25):
        p.accept(payload(i, i))
    check("pause: при inflight >= max_inflight чтение приостановлено", fake.paused and p.paused and fake.pauses == 1)
    await asyncio.sleep(0.8)
    check("pause: после записи (inflight <= 50 %) чтение возобновлено", not fake.paused and not p.paused
          and fake.resumes == 1 and p.inflight == 0)
    st = await p.stop_run()
    check("pause: ничего не потеряно, пауза учтена", st["stored"] == 25 and st["pause"]["count"] == 1
          and st["pause"]["seconds"] > 0.1, f"(пауза {st['pause']['seconds']:.2f} с)")
    await p.stop()

    p, fake = await make_pipeline(tmp, batch_size=1000, max_delay_s=30, policy="none")
    await p.start_run("t-none", {})
    for i in range(500):
        p.accept(payload(i, i))
    await asyncio.sleep(0.3)
    check("none: очередь растёт без ограничения, пауз нет", p.inflight == 500 and fake.pauses == 0)
    n = await p.discard()
    check("discard: буфер очищен, сброшенное посчитано", n == 500 and p.inflight == 0 and p.m.dropped["discarded"] == 500)
    await p.stop()


def test_policies():
    with tempfile.TemporaryDirectory() as tmp:
        asyncio.run(_policies(tmp))


async def _metrics(tmp):
    p, _ = await make_pipeline(tmp, batch_size=10, max_delay_s=0.2)
    await p.start_run("t-m", {})
    for i in range(30):
        p.accept(payload(i, i))
    await asyncio.sleep(0.6)
    text = generate_latest(p.registry).decode()
    need = ["messages_received_total 30.0", "messages_stored_total 30.0", "queue_depth 0.0", "batch_size_bucket",
            "batch_size_count 3.0", "batch_size_sum 30.0", "processing_latency_seconds_bucket",
            "processing_latency_seconds_count 30.0", "e2e_latency_seconds_bucket", "process_cpu_seconds_total",
            "process_resident_memory_bytes", 'batches_flushed_total{reason="size"} 3.0', "inflight_messages 0.0"]
    missing = [n for n in need if n not in text]
    check("/metrics: есть все метрики из задания и корректные значения", not missing, f"(нет: {missing})")
    st = await p.stop_run()
    e = st["latency_all"]["e2e"]
    check("точные перцентили e2e: p50 <= p95 <= p99 <= max", e["p50"] <= e["p95"] <= e["p99"] <= e["max"] and e["n"] == 30)
    await p.stop()


def test_metrics():
    with tempfile.TemporaryDirectory() as tmp:
        asyncio.run(_metrics(tmp))


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            print(f"[{name}]")
            fn()
    print(f"\nПроверок: {len(OK)}")
    print("ВСЕ ТЕСТЫ ПРОЙДЕНЫ")
