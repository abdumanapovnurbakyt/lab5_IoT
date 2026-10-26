def percentile(sorted_vals, q):
    n = len(sorted_vals)
    if n == 0:
        return None
    if n == 1:
        return float(sorted_vals[0])
    pos = (n - 1) * q / 100.0
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return float(sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac)


def summarize(values):
    v = sorted(values)
    if not v:
        return {"n": 0, "mean": None, "p50": None, "p95": None, "p99": None, "max": None}
    return {"n": len(v), "mean": sum(v) / len(v), "p50": percentile(v, 50), "p95": percentile(v, 95),
            "p99": percentile(v, 99), "max": float(v[-1])}


def ols_slope(xs, ys):
    n = len(xs)
    if n < 3:
        return 0.0, 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0:
        return 0.0, 0.0
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    syy = sum((y - my) ** 2 for y in ys)
    slope = sxy / sxx
    r2 = (sxy * sxy) / (sxx * syy) if syy > 0 else 0.0
    return slope, r2


def hist_quantile(buckets, q):
    total = buckets[-1][1]
    if total <= 0:
        return None
    rank = q / 100.0 * total
    prev_le, prev_c = 0.0, 0
    for le, c in buckets:
        if c >= rank:
            if le == float("inf"):
                return prev_le
            if c == prev_c:
                return le
            return prev_le + (le - prev_le) * (rank - prev_c) / (c - prev_c)
        prev_le, prev_c = le, c
    return prev_le
