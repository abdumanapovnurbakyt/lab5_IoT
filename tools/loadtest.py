import argparse
import copy
import csv
import json
import math
import os
import platform
import re
import sys
import threading
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import paho.mqtt.client as mqtt  # noqa: E402

from app.stats import ols_slope  # noqa: E402
from tools import simulator  # noqa: E402

LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)")
SYS = {"bytes_in": "$SYS/broker/bytes/received", "bytes_out": "$SYS/broker/bytes/sent",
       "pub_in": "$SYS/broker/publish/messages/received", "pub_out": "$SYS/broker/publish/messages/sent",
       "dropped": "$SYS/broker/publish/messages/dropped", "msgs_in": "$SYS/broker/messages/received",
       "msgs_out": "$SYS/broker/messages/sent"}


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


class Ingest:
    def __init__(self, url):
        self.url = url.rstrip("/")

    def get(self, path, timeout=10):
        with urllib.request.urlopen(self.url + path, timeout=timeout) as r:
            return r.read()

    def post(self, path, body, timeout=180):
        req = urllib.request.Request(self.url + path, data=json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())

    def wait_ready(self, timeout=90):
        t = time.time()
        while time.time() - t < timeout:
            try:
                self.get("/healthz", 3)
                return True
            except Exception:                          
                time.sleep(1)
        return False


def parse_metrics(text):
    out = {}
    for ln in text.splitlines():
        if ln.startswith("#"):
            continue
        m = LINE.match(ln)
        if m:
            try:
                out[m.group(1) + (m.group(2) or "")] = float(m.group(3))
            except ValueError:
                pass
    return out


class Poller(threading.Thread):
    def __init__(self, ingest, interval=1.0):
        super().__init__(daemon=True)
        self.ingest, self.interval = ingest, interval
        self.samples, self.errors = [], 0
        self.halt = threading.Event()

    def run(self):
        nxt = time.time()
        while not self.halt.is_set():
            t = time.time()
            try:
                self.samples.append((t, parse_metrics(self.ingest.get("/metrics", 5).decode())))
            except Exception:                                    # noqa: BLE001
                self.errors += 1
            nxt += self.interval
            self.halt.wait(max(0.0, nxt - time.time()))


class Broker:
    def __init__(self, host, port):
        self.vals = {}
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="loadtest-sys")
        self.c.on_connect = lambda c, u, f, rc, p: c.subscribe([(t, 0) for t in SYS.values()])
        self.c.on_message = self._on
        for _ in range(60):
            try:
                self.c.connect(host, port, 60)
                break
            except OSError:
                time.sleep(1)
        self.c.loop_start()

    def _on(self, c, u, msg):
        try:
            self.vals[msg.topic] = float(msg.payload)
        except ValueError:
            pass

    def snap(self):
        return dict(self.vals)

    def delta(self, a, b):
        return {k: (b.get(t, 0.0) - a.get(t, 0.0)) for k, t in SYS.items()}


class Ctx:
    def __init__(self, a, exp):
        self.exp = exp
        self.common = exp["common"]
        self.ingest = Ingest(a.ingest)
        self.broker_host, self.broker_port = a.broker, a.broker_port
        self.broker = None
        self.seed = 1


def _at(samples, t, side):
    if side == "first":
        for s in samples:
            if s[0] >= t:
                return s
        return samples[-1]
    best = samples[0]
    for s in samples:
        if s[0] <= t:
            best = s
    return best


def _rate(samples, key, t0, t1):
    a, b = _at(samples, t0, "first"), _at(samples, t1, "last")
    dt = b[0] - a[0]
    return (b[1].get(key, 0.0) - a[1].get(key, 0.0)) / dt if dt > 0 else 0.0


def wait_drain(poller, t_after, timeout):
    t0 = time.time()
    while time.time() - t0 < timeout:
        time.sleep(0.5)
        tail = [s for s in poller.samples if s[0] > t_after][-3:]
        if len(tail) == 3 and all(s[1].get("inflight_messages", 1) == 0 and s[1].get("queue_depth", 1) == 0 for s in tail) \
                and tail[0][1].get("messages_received_total") == tail[2][1].get("messages_received_total"):
            return True, max(0.0, tail[0][0] - t_after)
    return False, time.time() - t0


def run_one(ctx, name, devices, rate, duration, warmup, drain_timeout, ingest_cfg, discard=False):
    ing = ctx.ingest
    body = {"run_id": name, "discard": discard, **ingest_cfg}
    try:
        info = ing.post("/run/start", body)
    except urllib.error.HTTPError as e:
        if e.code != 409:
            raise
        info = ing.post("/run/start", {**body, "discard": True})
    poller = Poller(ing)
    poller.start()
    sys0 = ctx.broker.snap()
    h = simulator.run_load(ctx.broker_host, ctx.broker_port, devices, rate, duration, procs=ctx.common["procs"],
                           qos=ctx.common["qos"], invalid_ratio=ctx.common["invalid_ratio"], seed=ctx.seed, lead_in=3.0)
    gen = simulator.collect_load(h)
    t_end = gen["end_at"]
    drained, drain_s = wait_drain(poller, t_end, drain_timeout)
    poller.halt.set()
    poller.join(timeout=5)
    time.sleep(1.3)                                         
    sys1 = ctx.broker.snap()
    tw0, tw1 = gen["start_at"] + warmup, t_end
    stop = ing.post("/run/stop", {"t0": tw0, "t1": tw1})
    res = analyze(name, devices, rate, duration, warmup, ingest_cfg, gen, poller.samples, stop,
                  ctx.broker.delta(sys0, sys1), drained, drain_s, info)
    log(f"  {name}: нагрузка {res['offered']:.0f}/с, записано {res['throughput']:.0f}/с, p95 e2e "
        f"{_fmt(res['e2e']['p95'])} с, очередь макс {res['queue_max']:.0f}, CPU {res['cpu_pct']:.0f} %, "
        f"потери {res['loss_pct']:.2f} %{'' if drained else ', НЕ ОПУСТОШЕНО'}"
        f"{', ГЕНЕРАТОР НЕ ДОГНАЛ' if res['generator_limited'] else ''}")
    return res


def _fmt(x):
    return "н/д" if x is None else f"{x:.3f}"


def analyze(name, devices, rate, duration, warmup, cfg, gen, samples, stop, brk, drained, drain_s, info):
    t0, t1 = gen["start_at"], gen["end_at"]
    tw0, tw1 = t0 + warmup, t1
    win = [s for s in samples if tw0 <= s[0] <= tw1]
    if len(win) < 3:
        win = [s for s in samples if t0 <= s[0] <= t1]
    ts = [s[0] - t0 for s in win]

    def col(key):
        return [s[1].get(key, 0.0) for s in win]

    q_slope, q_r2 = ols_slope(ts, col("queue_depth"))
    cpu = _rate(samples, "process_cpu_seconds_total", tw0, tw1) * 100.0
    busy = {"mqtt_read": _rate(samples, "mqtt_read_seconds_total", tw0, tw1),
            "process": _rate(samples, "process_seconds_total", tw0, tw1),
            "storage": _rate(samples, "storage_write_seconds_total", tw0, tw1)}
    series = {k: [] for k in ("t", "queue", "inflight", "stored_rate", "recv_rate", "cpu_pct", "rss_mb", "paused", "lag")}
    prev = None
    for t, d in samples:
        if t < t0 - 1:
            prev = (t, d)
            continue
        if prev is not None:
            dt = t - prev[0]
            if dt > 0:
                series["t"].append(t - t0)
                series["queue"].append(d.get("queue_depth", 0.0))
                series["inflight"].append(d.get("inflight_messages", 0.0))
                series["stored_rate"].append((d.get("messages_processed_total", 0) - prev[1].get("messages_processed_total", 0)) / dt)
                series["recv_rate"].append((d.get("messages_received_total", 0) - prev[1].get("messages_received_total", 0)) / dt)
                series["cpu_pct"].append((d.get("process_cpu_seconds_total", 0) - prev[1].get("process_cpu_seconds_total", 0)) / dt * 100)
                series["rss_mb"].append(d.get("process_resident_memory_bytes", 0) / 1e6)
                series["paused"].append(d.get("backpressure_paused", 0.0))
                series["lag"].append(d.get("event_loop_lag_seconds", 0.0))
        prev = (t, d)
    rss = [s[1].get("process_resident_memory_bytes", 0) / 1e6 for s in samples if s[0] >= t0 - 1]
    rss_w = [s[1].get("process_resident_memory_bytes", 0) / 1e6 for s in win] or [0.0]
    rss_slope, _ = ols_slope(ts, rss_w)

    db = stop["db"]
    storage_null = cfg.get("storage") == "null"
    stored = stop["stored"] if storage_null else db["distinct"]
    sent_valid = gen["sent_valid"]
    lost = sent_valid - stored
    shed = stop["dropped"]["shed"]
    discarded = stop["dropped"]["discarded"]
    lat = stop["latency_window"]
    batches = stop["flush"]
    nb = sum(batches.values())
    thr = _rate(samples, "messages_processed_total", tw0, tw1)
    recv = _rate(samples, "messages_received_total", tw0, tw1)
    return {
        "name": name, "devices": devices, "rate_hz": rate, "offered": devices * rate, "duration_s": duration,
        "warmup_s": warmup, "config": cfg,
        "generator": gen, "generator_limited": bool(gen["errors"] or gen["achieved_rate"] < 0.95 * gen["target_rate"]
                                                    or gen["lag_max_s"] > 1.0),
        "throughput": thr, "recv_rate": recv,
        "proc": lat["proc"], "e2e": lat["e2e"], "e2e_all": stop["latency_all"]["e2e"],
        "queue_max": max(col("queue_depth") or [0.0]), "queue_mean": (sum(col("queue_depth")) / len(win)) if win else 0.0,
        "queue_slope": q_slope, "queue_r2": q_r2, "inflight_max": max(col("inflight_messages") or [0.0]),
        "cpu_pct": cpu, "rss_start_mb": rss[0] if rss else 0.0, "rss_max_mb": max(rss) if rss else 0.0,
        "rss_end_mb": rss[-1] if rss else 0.0, "rss_slope_mb_s": rss_slope,
        "lag_max_s": max(col("event_loop_lag_seconds") or [0.0]), "busy": busy,
        "paused_share": (sum(col("backpressure_paused")) / len(win)) if win else 0.0,
        "batches": stop["batches"], "batch_mean": stop["batch_mean"], "batch_min": stop["batch_min"],
        "batch_max": stop["batch_max"], "flush": batches, "timeout_share": (batches["timeout"] / nb) if nb else None,
        "sent_valid": sent_valid, "sent_invalid": gen["sent_invalid"], "stored": stored,
        "invalid_ingest": stop["invalid"], "invalid_match": sum(stop["invalid"].values()) == gen["sent_invalid"],
        "duplicates": db["duplicates"], "lost": lost, "loss_pct": (lost / sent_valid * 100.0) if sent_valid else 0.0,
        "shed": shed, "discarded": discarded, "broker_dropped": brk["dropped"], "inflight_end": stop["inflight_end"],
        "paused": stop["pause"], "drained": drained, "drain_s": drain_s, "broker": brk,
        "series": series,
    }


def base_cfg(ctx, **over):
    c = ctx.common
    cfg = {"batch_size": c["batch_size"], "max_delay_s": c["max_delay_s"], "policy": "none",
           "max_inflight": ctx.exp["e3"]["max_inflight"], "resume_ratio": ctx.exp["e3"]["resume_ratio"],
           "storage": "sqlite", "log_batches": False, "validate_schema": True, "qos": 0}
    cfg.update(over)
    return cfg


def e1(ctx, R, quick):
    p = ctx.exp["e1"]
    log("E1: нагрузка по числу устройств")
    runs, prev_ok = [], True
    for n in p["devices"]:
        runs.append(run_one(ctx, f"e1-n{n}", n, p["rate_hz"], p["duration_s"], p["warmup_s"], p["drain_timeout_s"],
                            base_cfg(ctx, log_batches=True), discard=not prev_ok))
        prev_ok = runs[-1]["drained"]
    R["e1"] = {"rate_hz": p["rate_hz"], "runs": runs}


def e2(ctx, R, quick):
    p = ctx.exp["e2"]
    log("E2: поиск точки насыщения")
    runs, prev_ok = [], True
    for load in p["loads"]:
        r = run_one(ctx, f"e2-l{load}", p["devices"], load / p["devices"], p["duration_s"], p["warmup_s"],
                    p["drain_timeout_s"], base_cfg(ctx), discard=not prev_ok)
        prev_ok = r["drained"]
        gen_rate = r["generator"]["achieved_rate"]
        r["saturated"] = bool((r["queue_slope"] > p["slope_threshold"] * gen_rate and r["queue_r2"] > 0.5)
                              or r["throughput"] < p["throughput_gap"] * gen_rate)
        runs.append(r)
    ok = [r for r in runs if not r["generator_limited"]]
    cap = max((r["throughput"] for r in ok), default=max(r["throughput"] for r in runs))
    sat = [r for r in ok if r["saturated"]]
    first = min(sat, key=lambda r: r["offered"]) if sat else None
    last_ok = max((r for r in ok if not r["saturated"] and (first is None or r["offered"] < first["offered"])),
                  key=lambda r: r["offered"], default=None)
    R["e2"] = {"runs": runs, "capacity": cap, "first_saturated_load": first["offered"] if first else None,
               "last_stable_load": last_ok["offered"] if last_ok else None, "devices": p["devices"],
               "found": first is not None}
    log(f"  ёмкость (плато записи) ≈ {cap:.0f} сообщ./с; первая перегруженная ступень: "
        f"{first['offered']:.0f}/с" if first else f"  ёмкость ≈ {cap:.0f}/с; насыщение не достигнуто (генератор слабее)")


def capacity_of(ctx, R):
    cap = R.get("e2", {}).get("capacity")
    return cap if cap else ctx.exp["e3"]["fallback_capacity"]


def e3(ctx, R, quick):
    p = ctx.exp["e3"]
    cap = capacity_of(ctx, R)
    over = max(100, round(cap * p["overload_factor"], -2))
    below = max(100, round(cap * p["below_factor"], -2))
    log(f"E3: перегрузка (ёмкость ≈ {cap:.0f}/с, перегрузка {over:.0f}/с, ниже насыщения {below:.0f}/с)")
    n = p["devices"]
    out = {"capacity": cap, "overload_load": over, "below_load": below, "runs": {}}
    prev_ok = True
    plan = [("below_pause", below, "pause", 0), ("over_none", over, "none", 0), ("over_pause", over, "pause", 0),
            ("over_shed", over, "shed", 0), ("over_none_qos1", over, "none", 1)]
    for key, load, policy, qos in plan:
        r = run_one(ctx, f"e3-{key}", n, load / n, p["duration_s"], p["warmup_s"], p["drain_timeout_s"],
                    base_cfg(ctx, policy=policy, qos=qos), discard=not prev_ok)
        prev_ok = r["drained"]
        out["runs"][key] = r
    # прогноз времени до нехватки памяти для режима без ограничений
    r = out["runs"]["over_none"]
    limit = ctx.exp["ingest_limits"]["mem_limit_mb"]
    if r["rss_slope_mb_s"] > 0.05:
        out["oom_eta_s"] = max(0.0, (limit - r["rss_end_mb"]) / r["rss_slope_mb_s"])
    else:
        out["oom_eta_s"] = None
    R["e3"] = out


def e4(ctx, R, quick):
    p = ctx.exp["e4"]
    log("E4: эффект батчинга")
    out = {"devices": p["devices"], "policy": p["policy"], "max_inflight": p["max_inflight"], "runs": []}
    prev_ok = True
    for b in p["batch_sizes"]:
        for load in p["loads"]:
            r = run_one(ctx, f"e4-b{b}-l{load}", p["devices"], load / p["devices"], p["duration_s"], p["warmup_s"],
                        p["drain_timeout_s"], base_cfg(ctx, batch_size=b, policy=p["policy"],
                                                      max_inflight=p["max_inflight"]), discard=not prev_ok)
            prev_ok = r["drained"]
            r["batch_size_cfg"] = b
            out["runs"].append(r)
    R["e4"] = out


def e5(ctx, R, quick):
    p = ctx.exp["e5"]
    e3p = ctx.exp["e3"]
    cap = capacity_of(ctx, R)
    load = max(100, round(cap * e3p["overload_factor"], -2))
    log(f"E5: диагностика узкого места при нагрузке {load:.0f}/с")
    out = {"load": load, "runs": []}
    prev_ok = True
    for b, st, val in ((500, "sqlite", True), (500, "null", True), (500, "sqlite", False), (50, "sqlite", True),
                       (1, "sqlite", True), (1, "null", True)):
        tag = f"e5-b{b}-{st}" + ("" if val else "-novalidate")
        r = run_one(ctx, tag, p["devices"], load / p["devices"], p["duration_s"], p["warmup_s"],
                    p["drain_timeout_s"], base_cfg(ctx, batch_size=b, storage=st, validate_schema=val, policy="pause",
                                                  max_inflight=2000), discard=not prev_ok)
        prev_ok = r["drained"]
        r["batch_size_cfg"], r["storage_cfg"], r["validate_cfg"] = b, st, val
        out["runs"].append(r)
    R["e5"] = out


def _packet_sizes(seq_max, topic_len, qos):
    """Средний размер PUBLISH-пакета для сообщений с seq из [0, seq_max): seq растёт в ходе опыта, и число цифр
    в нём влияет на размер (в 1-м опыте seq < 100, значит, 1-2 цифры)."""
    rng = __import__("random").Random(1)
    sizes = [len(simulator.make_payload(i % 2000, i % max(seq_max, 1), time.time(), rng)[0]) for i in range(4000)]
    payload = sum(sizes) / len(sizes)
    rem = 2 + topic_len + (2 if qos else 0) + payload        # длина имени топика + имя + идентификатор пакета + данные
    return payload, 1 + (1 if rem < 128 else 2) + rem        # + фиксированный заголовок (1 байт тип + длина)


def traffic(R, ctx):
    """Расчётный трафик на уровне MQTT против измеренного брокером ($SYS/broker/bytes/*) - для сравнения с практикой 8.

    Ожидаемо на одно сообщение: в брокер идёт PUBLISH QoS 1; из брокера - PUBLISH QoS 0 в ingest (на 2 байта короче:
    нет идентификатора пакета) и PUBACK (4 байта) обратно устройству. Заголовки TCP/IP сюда не входят - их видно
    только в Wireshark (+40...52 байта на сегмент и отдельные ACK)."""
    topic = len("telemetry/dev-00000")
    qos = ctx.common["qos"]
    rows = []
    for r in R.get("e1", {}).get("runs", []):
        g = r["generator"]
        msgs = g["sent_total"]
        seq_max = int(msgs / max(r["devices"], 1))
        payload, pub = _packet_sizes(seq_max, topic, qos)
        exp_in = pub
        exp_out = (pub - (2 if qos else 0)) + (4 if qos else 0)
        b = r["broker"]
        m_in = (b["bytes_in"] / b["pub_in"]) if b["pub_in"] else None
        m_out = (b["bytes_out"] / b["pub_out"]) if b["pub_out"] else None
        rate = g["achieved_rate"]
        rows.append({"devices": r["devices"], "offered": r["offered"], "measured_sent_rate": rate,
                     "measured_stored_rate": r["throughput"], "msgs": msgs, "seq_max": seq_max,
                     "payload_bytes": payload, "publish_packet_bytes": pub,
                     "expected_bytes_per_msg_in": exp_in, "measured_bytes_per_msg_in": m_in,
                     "expected_bytes_per_msg_out": exp_out, "measured_bytes_per_msg_out": m_out,
                     "expected_kBps_in": r["offered"] * exp_in / 1000.0,
                     "measured_kBps_in": (rate * m_in / 1000.0) if m_in else None,
                     "expected_kBps_out": r["offered"] * exp_out / 1000.0,
                     "measured_kBps_out": (rate * m_out / 1000.0) if m_out else None})
    return {"topic_bytes": topic, "puback_bytes": 4 if qos else 0, "qos": qos, "rows": rows}


def clean(o):
    if isinstance(o, dict):
        return {str(k): clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [clean(v) for v in o]
    if isinstance(o, float):
        return None if (math.isnan(o) or math.isinf(o)) else o
    return o


def save(R, out):
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "results.json"), "w", encoding="utf-8") as f:
        json.dump(clean(R), f, ensure_ascii=False, indent=1, allow_nan=False)


def _c(x, d=1):
    return "" if x is None else f"{x:.{d}f}".replace(".", ",")


def write_csv(R, out):
    def w(name, header, rows):
        with open(os.path.join(out, name), "w", newline="", encoding="utf-8-sig") as f:
            cw = csv.writer(f, delimiter=";")
            cw.writerow(header)
            cw.writerows(rows)

    def row(r, extra=()):
        e = r["e2e"]
        return [*extra, _c(r["offered"], 0), _c(r["throughput"], 0), _c(e["p50"], 3), _c(e["p95"], 3), _c(e["p99"], 3),
                _c(r["queue_max"], 0), _c(r["queue_slope"], 1), _c(r["cpu_pct"], 0), _c(r["rss_max_mb"], 0),
                _c(r["batch_mean"], 1), _c(r["loss_pct"], 2)]

    hdr = ["offered_msg_s", "throughput_msg_s", "e2e_p50_s", "e2e_p95_s", "e2e_p99_s", "queue_max", "queue_slope_msg_s",
           "cpu_pct", "rss_max_mb", "batch_mean", "loss_pct"]
    if "e1" in R:
        w("e1_load.csv", ["devices", *hdr], [row(r, [r["devices"]]) for r in R["e1"]["runs"]])
    if "e2" in R:
        w("e2_saturation.csv", [*hdr, "saturated"], [row(r) + ["yes" if r["saturated"] else "no"] for r in R["e2"]["runs"]])
    if "e3" in R:
        w("e3_overload.csv", ["scenario", *hdr, "broker_dropped", "shed", "rss_slope_mb_s"],
          [row(r, [k]) + [_c(r["broker_dropped"], 0), _c(r["shed"], 0), _c(r["rss_slope_mb_s"], 2)]
           for k, r in R["e3"]["runs"].items()])
    if "e4" in R:
        w("e4_batching.csv", ["batch_size", *hdr], [row(r, [r["batch_size_cfg"]]) for r in R["e4"]["runs"]])
    if "e5" in R:
        w("e5_bottleneck.csv", ["batch_size", "storage", "validation", *hdr, "busy_read", "busy_process", "busy_storage"],
          [row(r, [r["batch_size_cfg"], r["storage_cfg"], "yes" if r["validate_cfg"] else "no"]) +
           [_c(r["busy"]["mqtt_read"], 2), _c(r["busy"]["process"], 2), _c(r["busy"]["storage"], 2)]
           for r in R["e5"]["runs"]])


def environment(ctx):
    model = ""
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for ln in f:
                if ln.startswith("model name"):
                    model = ln.split(":", 1)[1].strip()
                    break
    except OSError:
        pass
    import importlib.metadata as md
    return {"cpu_model": model, "cpu_count": os.cpu_count(), "python": platform.python_version(),
            "platform": platform.platform(), "limits": ctx.exp["ingest_limits"],
            "versions": {k: md.version(k) for k in ("paho-mqtt", "fastapi", "uvicorn", "jsonschema", "prometheus_client")},
            "started": time.strftime("%Y-%m-%d %H:%M:%S")}


def smoke(ctx):
    log("SMOKE: 100 устройств, 5 сообщ./с, 15 с")
    r = run_one(ctx, "smoke", 100, 5.0, 15, 3, 30, base_cfg(ctx, log_batches=True))
    ok = (r["lost"] == 0 and r["duplicates"] == 0 and r["invalid_match"] and r["drained"] and not r["generator_limited"])
    log("SMOKE:", "ОК - потерь нет, дубликатов нет, невалидные отклонены" if ok else f"ПРОВАЛ: {json.dumps({k: r[k] for k in ('lost', 'duplicates', 'invalid_match', 'drained', 'generator_limited')})}")
    return 0 if ok else 1


def apply_quick(exp):
    exp["e1"].update(duration_s=12, warmup_s=3, drain_timeout_s=30)
    exp["e2"].update(loads=[1000, 2000, 4000, 8000], duration_s=10, warmup_s=3, drain_timeout_s=10)
    exp["e3"].update(duration_s=20, warmup_s=4, drain_timeout_s=45)
    exp["e4"].update(loads=[500, 2000], duration_s=10, warmup_s=3, drain_timeout_s=25)
    exp["e5"].update(duration_s=10, warmup_s=3, drain_timeout_s=10)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ingest", default=os.environ.get("INGEST_URL", "http://ingest:8000"))
    ap.add_argument("--broker", default=os.environ.get("MQTT_HOST", "mosquitto"))
    ap.add_argument("--broker-port", type=int, default=1883)
    ap.add_argument("--out", default=os.path.join(ROOT, "results"))
    ap.add_argument("--exp", default=os.path.join(ROOT, "experiments.json"))
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--smoke", action="store_true")
    ap.add_argument("--only", default="", help="например: e1,e3")
    ap.add_argument("--no-plots", action="store_true")
    a = ap.parse_args()
    with open(a.exp, encoding="utf-8") as f:
        exp = json.load(f)
    if a.quick:
        apply_quick(exp)
    ctx = Ctx(a, exp)
    if not ctx.ingest.wait_ready():
        log("ingest-сервис не отвечает на", a.ingest)
        return 2
    ctx.broker = Broker(a.broker, a.broker_port)
    time.sleep(2.0)
    if a.smoke:
        return smoke(ctx)
    path = os.path.join(a.out, "results.json")
    R = {}
    if a.only and os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            R = json.load(f)                               
    R["meta"] = {**environment(ctx), "quick": a.quick, "experiments": exp}
    todo = [s.strip() for s in a.only.split(",") if s.strip()] or ["e1", "e2", "e3", "e4", "e5"]
    t_start = time.time()
    funcs = {"e1": e1, "e2": e2, "e3": e3, "e4": e4, "e5": e5}
    for k in todo:
        funcs[k](ctx, R, a.quick)
        R["traffic"] = traffic(R, ctx)
        save(R, a.out)
    R["meta"]["elapsed_s"] = time.time() - t_start
    save(R, a.out)
    write_csv(R, a.out)
    if not a.no_plots:
        import tools.plots as plots
        plots.make_all(R, a.out)
    log(f"Готово за {time.time() - t_start:.0f} с. Результаты: {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
