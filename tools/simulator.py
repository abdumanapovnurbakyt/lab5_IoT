import argparse
import asyncio
import heapq
import json
import multiprocessing as mp
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.mqtt_async import AsyncMQTT  # noqa: E402


def make_payload(idx, seq, now, rng, invalid_ratio=0.0):
    """Возвращает (bytes, valid). Невалидные сообщения проверяют валидацию в ingest-сервисе."""
    msg = {"device_id": f"dev-{idx:05d}", "seq": seq, "ts": now,
           "temperature": round(rng.uniform(15.0, 30.0), 2),
           "humidity": round(rng.uniform(30.0, 70.0), 1),
           "battery_mv": rng.randint(3300, 4100)}
    if invalid_ratio and rng.random() < invalid_ratio:
        kind = rng.randrange(3)
        if kind == 0:
            msg["humidity"] = 140.0                
        elif kind == 1:
            del msg["battery_mv"]                 
        else:
            msg["extra"] = "x"                      
        return json.dumps(msg, separators=(",", ":")).encode(), False
    return json.dumps(msg, separators=(",", ":")).encode(), True


async def _worker_main(proc_idx, device_ids, rate, start_at, duration, qos, host, port, invalid_ratio, seed):
    rng = random.Random(seed * 1000 + proc_idx)
    cli = AsyncMQTT(host, port, f"sim-{os.getpid()}-{proc_idx}", qos=qos)
    cli.client.max_inflight_messages_set(5000)
    cli.client.max_queued_messages_set(0)
    acked = [0]
    cli.client.on_publish = lambda c, u, mid, rc, props: acked.__setitem__(0, acked[0] + 1)
    await cli.start()
    for _ in range(100):                                 
        if cli.connected:
            break
        await asyncio.sleep(0.05)
    period = 1.0 / rate
    seq = {d: 0 for d in device_ids}
    heap = [(start_at + rng.random() * period, d) for d in device_ids]
    heapq.heapify(heap)
    end = start_at + duration
    sent_valid = sent_invalid = late = 0
    lag_sum = lag_max = 0.0
    publish = cli.client.publish
    time_ = time.time
    await asyncio.sleep(max(0.0, start_at - time_()))
    while True:
        now = time_()
        if now >= end:
            break
        n = 0
        while heap and heap[0][0] <= now and heap[0][0] < end:
            t_due, d = heapq.heappop(heap)
            lag = now - t_due
            lag_sum += lag
            if lag > lag_max:
                lag_max = lag
            if lag > 0.1:
                late += 1
            payload, ok = make_payload(d, seq[d], time_(), rng, invalid_ratio)
            seq[d] += 1
            publish(f"telemetry/dev-{d:05d}", payload, qos=qos)
            if ok:
                sent_valid += 1
            else:
                sent_invalid += 1
            heapq.heappush(heap, (t_due + period, d))
            n += 1
            if n >= 200:                                 
                n = 0
                await asyncio.sleep(0)
                now = time_()
        await asyncio.sleep(0.002)
    sent = sent_valid + sent_invalid
    t_wait = time_()
    while acked[0] < sent and time_() - t_wait < 10.0 and qos > 0:
        await asyncio.sleep(0.05)
    await cli.stop()
    return {"proc": proc_idx, "devices": len(device_ids), "sent_valid": sent_valid, "sent_invalid": sent_invalid,
            "acked": acked[0], "lag_sum": lag_sum, "lag_max": lag_max, "late": late}


def _worker(args, q):
    try:
        q.put(asyncio.run(_worker_main(*args)))
    except Exception as e:                              
        q.put({"error": repr(e), "proc": args[0]})


def run_load(host, port, devices, rate, duration, procs=2, qos=1, invalid_ratio=0.005, seed=1, lead_in=1.5):
    procs = max(1, min(procs, devices))
    ids = list(range(devices))
    parts = [ids[i::procs] for i in range(procs)]
    start_at = time.time() + lead_in
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    ps = [ctx.Process(target=_worker, args=((i, parts[i], rate, start_at, duration, qos, host, port, invalid_ratio,
                                             seed), q)) for i in range(procs)]
    for p in ps:
        p.start()
    return {"procs": ps, "queue": q, "start_at": start_at, "end_at": start_at + duration, "n_procs": procs,
            "devices": devices, "rate": rate, "duration": duration}


def collect_load(handle, timeout=None):
    res = []
    deadline = time.time() + (timeout if timeout is not None else handle["duration"] + 60)
    while len(res) < handle["n_procs"] and time.time() < deadline:
        try:
            res.append(handle["queue"].get(timeout=0.5))
        except Exception:                               
            pass
    for p in handle["procs"]:
        p.join(timeout=5)
        if p.is_alive():
            p.terminate()
    errs = [r["error"] for r in res if "error" in r]
    ok = [r for r in res if "error" not in r]
    sv = sum(r["sent_valid"] for r in ok)
    si = sum(r["sent_invalid"] for r in ok)
    total = sv + si
    return {
        "errors": errs, "sent_valid": sv, "sent_invalid": si, "sent_total": total,
        "acked": sum(r["acked"] for r in ok),
        "target_rate": handle["devices"] * handle["rate"],
        "achieved_rate": total / handle["duration"] if handle["duration"] else 0.0,
        "lag_mean_s": (sum(r["lag_sum"] for r in ok) / total) if total else 0.0,
        "lag_max_s": max((r["lag_max"] for r in ok), default=0.0),
        "late_share": (sum(r["late"] for r in ok) / total) if total else 0.0,
        "start_at": handle["start_at"], "end_at": handle["end_at"],
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default=os.environ.get("MQTT_HOST", "mosquitto"))
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--devices", type=int, default=500)
    ap.add_argument("--rate", type=float, default=2.0, help="сообщений в секунду на устройство")
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--procs", type=int, default=2)
    ap.add_argument("--qos", type=int, default=1, choices=(0, 1))
    ap.add_argument("--invalid-ratio", type=float, default=0.005)
    a = ap.parse_args()
    h = run_load(a.host, a.port, a.devices, a.rate, a.duration, a.procs, a.qos, a.invalid_ratio)
    r = collect_load(h)
    print(json.dumps(r, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
