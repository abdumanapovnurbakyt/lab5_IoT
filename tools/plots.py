import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Patch  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

SURFACE, INK, INK2, MUTED, GRID, BASE = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
CAT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
NB = " "

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "DejaVu Sans", "font.size": 9.5, "text.color": INK,
    "axes.labelcolor": INK2, "axes.labelsize": 9.5, "xtick.color": MUTED, "ytick.color": MUTED,
    "xtick.labelcolor": INK2, "ytick.labelcolor": INK2, "axes.edgecolor": BASE,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "axes.grid.axis": "y",
    "grid.color": GRID, "grid.linewidth": 0.7, "axes.axisbelow": True,
    "axes.titlesize": 10.5, "axes.titleweight": "bold", "axes.titlelocation": "left", "axes.titlepad": 9,
    "legend.frameon": False, "legend.fontsize": 9, "lines.linewidth": 1.7,
    "xtick.major.size": 3, "ytick.major.size": 3,
})

SCEN = {"over_none": ("без ограничений", CAT[0], "o"), "over_pause": ("backpressure (pause)", CAT[1], "s"),
        "over_shed": ("сброс нагрузки (shed)", CAT[2], "^"), "over_none_qos1": ("без ограничений, QoS 1", CAT[4], "D")}
BATCH = {1: (CAT[0], "o"), 50: (CAT[1], "s"), 500: (CAT[2], "^")}


def ru(x, d=1):
    return f"{x:.{d}f}".replace(".", ",")


def n_fmt(v, _=None):
    return f"{int(round(v)):,}".replace(",", NB)


def arr(x):
    return np.array([np.nan if v is None else v for v in x], dtype=float)


def clean_axes(ax, grid_x=False):
    ax.spines["left"].set_color(BASE)
    ax.spines["bottom"].set_color(BASE)
    if grid_x:
        ax.grid(axis="x", color=GRID, lw=0.7)


def save(fig, out, name):
    d = os.path.join(out, "figures")
    os.makedirs(d, exist_ok=True)
    p = os.path.join(d, name)
    fig.savefig(p, dpi=160)
    plt.close(fig)
    return p


def sec_fmt(v, _=None):
    if v >= 10:
        return f"{v:.0f}"
    if v >= 1:
        return f"{v:g}".replace(".", ",")
    return f"{v:g}".replace(".", ",")


def fig_architecture(R, out):
    fig = plt.figure(figsize=(11.4, 4.0))
    ax = fig.add_axes([0, 0, 1, 1])
    W, G, H, Y = 15.5, 6.5, 11.0, 15.0
    ax.set_xlim(0, 6 * W + 5 * G + 2)
    ax.set_ylim(0, 36)
    ax.axis("off")
    xs = [1 + i * (W + G) for i in range(6)]

    def box(i, txt, fc="#e8f0fb", ec=CAT[0], y=Y, h=H):
        ax.add_patch(FancyBboxPatch((xs[i], y), W, h, boxstyle="round,pad=0.3,rounding_size=1.2", fc=fc, ec=ec, lw=1.2))
        ax.text(xs[i] + W / 2, y + h / 2, txt, ha="center", va="center", fontsize=8.4, color=INK, linespacing=1.3)

    box(0, "simulator × N\nустройств\n(JSON, 1–3 Гц)")
    box(1, "mosquitto\n(брокер MQTT)", fc="#fdeee7", ec=CAT[1])
    box(2, "1. приём\nчтение сокета →\nочередь raw")
    box(3, "2. обработка\nJSON → JSON Schema\n→ обогащение")
    box(4, "3. батчинг\n500 сообщений\nИЛИ 2 с")
    box(5, "хранилище SQLite\n(один commit\nна пакет)", fc="#e3f5ee", ec=CAT[2])
    for i in range(5):
        ax.add_patch(FancyArrowPatch((xs[i] + W + 0.4, Y + H / 2), (xs[i + 1] - 0.4, Y + H / 2), arrowstyle="-|>",
                                     mutation_scale=11, color=INK2, lw=1.1))
    ax.text((xs[0] + W + xs[1]) / 2, Y + H / 2 + 1.6, "MQTT\nQoS 1", ha="center", fontsize=7.5, color=INK2)
    ax.text((xs[1] + W + xs[2]) / 2 - 1.4, Y + H / 2 + 1.6, "MQTT\nQoS 0", ha="center", fontsize=7.5, color=INK2)
    x0, x1 = xs[2] - 1.2, xs[4] + W + 1.2
    ax.add_patch(FancyBboxPatch((x0, Y - 3.2), x1 - x0, H + 9.2, boxstyle="round,pad=0.2,rounding_size=1.5", fc="none",
                                ec=MUTED, lw=1.0, ls=(0, (4, 3))))
    ax.text(x0 + 1, Y + H + 3.8, "ingest-service (Python, asyncio, FastAPI)", fontsize=9, color=INK2, fontweight="bold")
    # метрики и нагрузочный стенд
    ax.add_patch(FancyBboxPatch((xs[3], 1), W + G + 4, 8.2, boxstyle="round,pad=0.3,rounding_size=1.2", fc="#f6f4ea", ec=CAT[3], lw=1.2))
    ax.text(xs[3] + (W + G + 4) / 2, 5.1, "/metrics → Prometheus\nqueue_depth, batch_size, задержки", ha="center", va="center", fontsize=8.5)
    ax.add_patch(FancyArrowPatch((xs[3] + W / 2, Y - 3.2), (xs[3] + W / 2, 9.7), arrowstyle="-|>", mutation_scale=10, color=INK2, lw=1.0))
    ax.add_patch(FancyBboxPatch((xs[0], 1), 2 * W + G, 8.2, boxstyle="round,pad=0.3,rounding_size=1.2", fc="#f6f4ea", ec=CAT[3], lw=1.2))
    ax.text(xs[0] + (2 * W + G) / 2, 5.1, "loadtest: 100…2000 устройств; раз в секунду\nчитает /metrics и $SYS брокера", ha="center", va="center", fontsize=8.5)
    ax.text(xs[5] + W / 2, 5.1, "backpressure:\nпауза чтения сокета\nпри inflight ≥ max", ha="center", va="center", fontsize=8.3, color=INK2)
    return save(fig, out, "fig0_architecture.png")


def fig_load(R, out):
    runs = R["e1"]["runs"]
    lim = R["meta"]["limits"]
    x = np.arange(len(runs))
    labels = [n_fmt(r["devices"]) for r in runs]
    fig, axs = plt.subplots(2, 3, figsize=(11.6, 7.2))

    ax = axs[0, 0]
    ax.plot(x, [r["offered"] for r in runs], color=MUTED, ls=(0, (4, 2)), lw=1.3, marker="o", ms=4, label="нагрузка")
    ax.plot(x, [r["throughput"] for r in runs], color=CAT[0], marker="s", ms=6, label="обработано")
    ax.set_title("A. Пропускная способность")
    ax.set_ylabel("сообщений в секунду")
    ax.yaxis.set_major_formatter(FuncFormatter(n_fmt))
    ax.legend(loc="upper left")

    ax = axs[0, 1]
    for k, c, mk, lab in (("p50", CAT[0], "o", "p50"), ("p95", CAT[1], "s", "p95"), ("p99", CAT[2], "^", "p99")):
        y = arr([r["e2e"][k] for r in runs])
        ax.plot(x, y, color=c, marker=mk, ms=6, label=lab)
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(FuncFormatter(sec_fmt))
    ax.set_title("B. Задержка устройство → запись")
    ax.set_ylabel("секунд (лог. шкала)")
    ax.legend(loc="lower left")

    ax = axs[0, 2]
    ax.plot(x, [r["queue_max"] for r in runs], color=CAT[0], marker="o", ms=6)
    ax.axhline(R["meta"]["experiments"]["common"]["batch_size"], color=MUTED, ls=":", lw=1.0)
    ax.text(0.04, 0.5, "пунктир – размер\nпакета (500)", color=INK2, fontsize=8.5,
            va="center", transform=ax.transAxes)
    ax.set_title("C. Очередь сервиса (максимум)")
    ax.set_ylabel("сообщений")
    ax.yaxis.set_major_formatter(FuncFormatter(n_fmt))

    ax = axs[1, 0]
    ax.plot(x, [r["cpu_pct"] for r in runs], color=CAT[0], marker="o", ms=6)
    ax.axhline(lim["cpus"] * 100, color=CAT[7], ls=(0, (4, 2)), lw=1.2)
    ax.text(0.02, lim["cpus"] * 100, f"лимит контейнера {lim['cpus'] * 100:.0f} %", color=INK2, fontsize=8.5, va="bottom",
            transform=ax.get_yaxis_transform())
    ax.set_ylim(0, max(100 * lim["cpus"] * 1.25, max(r["cpu_pct"] for r in runs) * 1.1))
    ax.set_title("D. Загрузка CPU ingest-сервиса")
    ax.set_ylabel("% одного ядра")

    ax = axs[1, 1]
    ax.plot(x, [r["rss_max_mb"] for r in runs], color=CAT[0], marker="o", ms=6)
    ax.axhline(lim["mem_limit_mb"], color=CAT[7], ls=(0, (4, 2)), lw=1.2)
    ax.text(0.02, lim["mem_limit_mb"], f"лимит {lim['mem_limit_mb']} МБ", color=INK2, fontsize=8.5, va="top",
            transform=ax.get_yaxis_transform())
    ax.set_ylim(0, lim["mem_limit_mb"] * 1.1)
    ax.set_title("E. Память (RSS, максимум)")
    ax.set_ylabel("МБ")

    ax = axs[1, 2]
    share = [(r["timeout_share"] or 0) * 100 for r in runs]
    ax.plot(x, share, color=CAT[0], marker="o", ms=6)
    for i, (r, v) in enumerate(zip(runs, share)):
        ax.annotate(f"{v:.1f} %".replace(".", ","), (i, v), textcoords="offset points", xytext=(0, 9),
                    ha="center", fontsize=8.5, color=INK2)
    ax.set_ylim(0, max(share) * 1.45 + 1)
    ax.set_title("F. Пакеты, закрытые по таймеру")
    ax.set_ylabel("% пакетов")
    for ax in axs.flat:
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_xlabel("Число устройств")
        clean_axes(ax)
    fig.subplots_adjust(left=0.07, right=0.985, top=0.94, bottom=0.08, wspace=0.28, hspace=0.36)
    return save(fig, out, "fig1_load.png")


def fig_saturation(R, out):
    e2 = R["e2"]
    runs = e2["runs"]
    off = np.array([r["offered"] for r in runs])
    cap = e2["capacity"]
    lim = R["meta"]["limits"]
    fig, axs = plt.subplots(2, 3, figsize=(12.0, 7.4))

    def mark(ax):
        if e2["first_saturated_load"]:
            ax.axvline(e2["first_saturated_load"], color=CAT[7], lw=1.0, ls=(0, (4, 2)))

    ax = axs[0, 0]
    ax.plot([0, off.max()], [0, off.max()], color=MUTED, ls=(0, (4, 2)), lw=1.2, label="идеально")
    ax.plot(off, [r["throughput"] for r in runs], color=CAT[0], marker="o", ms=6, label="обработано")
    ax.axhline(cap, color=CAT[1], lw=1.0, ls=":")
    ax.text(off.min(), cap * 1.03, f"ёмкость ≈ {n_fmt(round(cap, -1))} сообщ./с", color=INK2, fontsize=8.5, va="bottom",
            ha="left")
    mark(ax)
    ax.set_title("A. Обработано против нагрузки")
    ax.set_ylabel("обработано, сообщений/с")
    ax.yaxis.set_major_formatter(FuncFormatter(n_fmt))
    ax.legend(loc="lower right")

    ax = axs[0, 1]
    thr = R["meta"]["experiments"]["e2"]["slope_threshold"]
    ax.plot(off, [r["queue_slope"] for r in runs], color=CAT[0], marker="o", ms=6, label="рост очереди")
    ax.plot(off, [thr * r["generator"]["achieved_rate"] for r in runs], color=MUTED, ls=(0, (4, 2)), lw=1.2,
            label=f"порог насыщения ({thr * 100:.0f} % нагрузки)")
    mark(ax)
    ax.axhline(0, color=BASE, lw=0.8)
    ax.set_title("B. Скорость роста очереди")
    ax.set_ylabel("сообщений в секунду (наклон МНК)")
    ax.yaxis.set_major_formatter(FuncFormatter(n_fmt))
    ax.legend(loc="upper left")

    ax = axs[0, 2]
    for k, c, mk in (("p50", CAT[0], "o"), ("p95", CAT[1], "s"), ("p99", CAT[2], "^")):
        ax.plot(off, arr([r["e2e"][k] for r in runs]), color=c, marker=mk, ms=5.5, label=k)
    mark(ax)
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(FuncFormatter(sec_fmt))
    ax.set_title("C. Задержка устройство → запись")
    ax.set_ylabel("секунд (лог. шкала)")
    ax.legend(loc="upper left")

    ax = axs[1, 0]
    ax.plot(off, [r["cpu_pct"] for r in runs], color=CAT[0], marker="o", ms=6)
    ax.axhline(lim["cpus"] * 100, color=CAT[7], lw=1.2, ls=(0, (4, 2)))
    ax.text(off.min(), lim["cpus"] * 100 + 1.5, f"лимит контейнера {lim['cpus'] * 100:.0f} %", color=INK2, fontsize=8.5)
    mark(ax)
    ax.set_ylim(0, lim["cpus"] * 100 * 1.25)
    ax.set_title("D. Загрузка CPU сервиса")
    ax.set_ylabel("% одного ядра")

    ax = axs[1, 1]
    for k, c, mk, lab in (("mqtt_read", CAT[0], "o", "приём"), ("process", CAT[1], "s", "обработка (JSON, схема)"),
                          ("storage", CAT[2], "^", "запись в SQLite")):
        ax.plot(off, [r["busy"][k] * 100 for r in runs], color=c, marker=mk, ms=5.5, label=lab)
    mark(ax)
    ax.set_title("E. Занятость стадий")
    ax.set_ylabel("% времени, когда стадия занята")
    ax.legend(loc="upper left")

    ax = axs[1, 2]
    ax.plot(off, [r["rss_max_mb"] for r in runs], color=CAT[0], marker="o", ms=6)
    mark(ax)
    ax.set_ylim(0, lim["mem_limit_mb"] * 0.4)
    ax.set_title("F. Память (RSS, максимум)")
    ax.set_ylabel(f"МБ (лимит {lim['mem_limit_mb']} МБ)")
    for ax in axs.flat:
        ax.set_xlabel("Предложенная нагрузка, сообщений/с")
        ax.xaxis.set_major_formatter(FuncFormatter(n_fmt))
        ax.set_xticks([0, 4000, 8000, 12000])
        clean_axes(ax, grid_x=True)
    if e2["first_saturated_load"]:
        fig.text(0.5, 0.004, f"красная пунктирная линия – первая нагрузка ({n_fmt(e2['first_saturated_load'])} сообщ./с), "
                 "при которой очередь растёт и обработка не успевает", ha="center", fontsize=8.8, color=INK2)
    fig.subplots_adjust(left=0.075, right=0.985, top=0.94, bottom=0.115, wspace=0.3, hspace=0.38)
    return save(fig, out, "fig2_saturation.png")


def fig_overload(R, out):
    e3 = R["e3"]
    runs = e3["runs"]
    keys = [k for k in SCEN if k in runs]
    fig = plt.figure(figsize=(12.0, 8.0))
    gs = fig.add_gridspec(2, 3, left=0.145, right=0.985, top=0.94, bottom=0.14, wspace=0.34, hspace=0.46)
    axA, axB, axC = (fig.add_subplot(gs[0, i]) for i in range(3))
    axD = fig.add_subplot(gs[1, 0:2])
    axE = fig.add_subplot(gs[1, 2], sharey=axD)
    dur = R["meta"]["experiments"]["e3"]["duration_s"]
    for k in keys:
        lab, c, mk = SCEN[k]
        s = runs[k]["series"]
        t = np.array(s["t"])
        ls = (0, (4, 2)) if k == "over_none_qos1" else "-"
        axA.plot(t, s["queue"], color=c, ls=ls, lw=1.6)
        axB.plot(t, s["stored_rate"], color=c, ls=ls, lw=1.2)
        axC.plot(t, s["rss_mb"], color=c, ls=ls, lw=1.6)
    axA.set_title("A. Очередь сервиса")
    axA.set_ylabel("сообщений")
    axA.yaxis.set_major_formatter(FuncFormatter(n_fmt))
    axB.set_title("B. Обработано в секунду")
    axB.set_ylabel("сообщений/с")
    axB.yaxis.set_major_formatter(FuncFormatter(n_fmt))
    axC.set_title("C. Память (RSS)")
    axC.set_ylabel("МБ")
    for ax in (axA, axB, axC):
        ax.axvline(dur, color=MUTED, lw=0.9, ls=":")
        ax.set_xlabel("секунд от начала нагрузки")
        ax.set_xlim(-1, dur + 30)
        clean_axes(ax, grid_x=True)
    axA.text(dur + 1, axA.get_ylim()[1] * 0.55, "конец\nнагрузки", fontsize=8, color=INK2, va="top")

    y = np.arange(len(keys))[::-1]
    for yy, k in zip(y, keys):
        r = runs[k]
        vals = [r["e2e"][q] for q in ("p50", "p99")]
        if None not in vals:
            axD.plot(vals, [yy, yy], color=BASE, lw=2.2, zorder=1)
        for key, mk, c in (("p50", "o", CAT[0]), ("p95", "s", CAT[1]), ("p99", "^", CAT[2])):
            v = r["e2e"][key]
            if v is not None:
                axD.plot([v], [yy], marker=mk, color=c, ms=7.5, ls="none", mec=SURFACE, zorder=2)
        axD.text(r["e2e"]["p99"] * 1.18, yy, f"p99 = {sec_s_(r['e2e']['p99'])}", va="center", fontsize=8.5, color=INK2)
    axD.set_xscale("log")
    axD.set_xlim(0.5, 60)
    axD.xaxis.set_major_formatter(FuncFormatter(sec_fmt))
    axD.set_yticks(y)
    axD.set_yticklabels([SCEN[k][0].replace(", QoS 1", ",\nQoS 1") for k in keys])
    axD.set_ylim(-0.6, len(keys) - 0.4)
    axD.set_xlabel("задержка устройство → запись, секунд (лог. шкала)")
    axD.set_title("D. Задержка сообщений, дошедших до записи")
    axD.grid(axis="x", color=GRID, lw=0.7)
    axD.grid(axis="y", visible=False)
    axD.legend(handles=[Line2D([], [], marker=m, color=c, ls="none", ms=7, label=l)
                        for l, m, c in (("p50", "o", CAT[0]), ("p95", "s", CAT[1]), ("p99", "^", CAT[2]))],
               loc="lower right", ncol=3, columnspacing=0.9, handletextpad=0.2)
    clean_axes(axD)

    for yy, k in zip(y, keys):
        r = runs[k]
        sv = max(r["sent_valid"], 1)
        stored = r["stored"] / sv * 100
        lost = max(0.0, 100 - stored)
        c = SCEN[k][1]
        axE.barh(yy, stored, height=0.55, color=c, edgecolor=SURFACE, linewidth=1.2)
        if lost > 0.5:
            axE.barh(yy, lost, left=stored, height=0.55, color=SURFACE, edgecolor=c, hatch="////", linewidth=1.2)
            axE.text(stored + lost / 2, yy, f"{lost:.0f}", ha="center", va="center", fontsize=9, fontweight="bold",
                     bbox=dict(boxstyle="round,pad=0.15", fc=SURFACE, ec="none"))
        axE.text(stored / 2, yy, f"{stored:.0f}", ha="center", va="center", fontsize=9, fontweight="bold", color="white")
    axE.set_xlim(0, 100)
    plt.setp(axE.get_yticklabels(), visible=False)
    axE.tick_params(axis="y", length=0)
    axE.set_xlabel("% сообщений устройств")
    axE.set_title("E. Записано и потеряно")
    axE.grid(axis="x", color=GRID, lw=0.7)
    axE.grid(axis="y", visible=False)
    clean_axes(axE)

    h = [Line2D([], [], color=SCEN[k][1], lw=2.0, ls=(0, (4, 2)) if k == "over_none_qos1" else "-", label=SCEN[k][0])
         for k in keys]
    h.append(Patch(fc=SURFACE, ec=INK2, hatch="////", label="штриховка – потеряно (у «backpressure» и «QoS 1» – брокером, у «shed» – сервисом)"))
    fig.legend(handles=h, loc="lower center", ncol=3, bbox_to_anchor=(0.56, 0.0), fontsize=8.8, columnspacing=1.6)
    return save(fig, out, "fig3_overload.png")


def sec_s_(x):
    return (f"{x:.0f}" if x >= 10 else f"{x:.1f}").replace(".", ",") + " с"


def fig_batching(R, out):
    e4 = R["e4"]
    runs = e4["runs"]
    bs = sorted({r["batch_size_cfg"] for r in runs})
    fig, axs = plt.subplots(2, 2, figsize=(11.2, 7.6))
    for b in bs:
        rr = sorted([r for r in runs if r["batch_size_cfg"] == b], key=lambda r: r["offered"])
        c, mk = BATCH[b]
        off = [r["offered"] for r in rr]
        axs[0, 0].plot(off, [r["throughput"] for r in rr], color=c, marker=mk, ms=6, label=f"batch = {b}")
        axs[0, 1].plot(off, arr([r["e2e"]["p50"] for r in rr]), color=c, marker=mk, ms=6, label=f"batch = {b}")
        axs[1, 0].plot(off, arr([r["e2e"]["p99"] for r in rr]), color=c, marker=mk, ms=6, label=f"batch = {b}")
        axs[1, 1].plot([r["throughput"] for r in rr], arr([r["e2e"]["p99"] for r in rr]), color=c, marker=mk, ms=6,
                       label=f"batch = {b}")
    mx = max(r["offered"] for r in runs)
    axs[0, 0].plot([0, mx], [0, mx], color=MUTED, ls=(0, (4, 2)), lw=1.2)
    axs[0, 0].text(mx * 0.62, mx * 0.69, "идеально", color=INK2, fontsize=8.5, rotation=0)
    axs[0, 0].set_title("A. Пропускная способность")
    axs[0, 0].set_ylabel("обработано, сообщений/с")
    axs[0, 1].set_title("B. Задержка p50")
    axs[1, 0].set_title("C. Задержка p99")
    axs[1, 1].set_title("D. Компромисс: p99 против пропускной способности")
    for ax in (axs[0, 1], axs[1, 0]):
        ax.set_yscale("log")
        ax.yaxis.set_major_formatter(FuncFormatter(sec_fmt))
        ax.set_ylabel("секунд (лог. шкала)")
    r1 = sorted([r for r in runs if r["batch_size_cfg"] == 1], key=lambda r: r["offered"])
    if r1:
        cap1 = max(r["throughput"] for r in r1)
        axs[1, 1].annotate("пакет 1 упирается в запись:\nпредел ≈ %s сообщ./с" % n_fmt(round(cap1, -1)),
                           (cap1, r1[-1]["e2e"]["p99"]), xytext=(0.5, 0.72), textcoords="axes fraction", fontsize=8.8,
                           color=INK2, arrowprops=dict(arrowstyle="-", color=MUTED, lw=0.8))
    axs[1, 1].set_yscale("log")
    axs[1, 1].yaxis.set_major_formatter(FuncFormatter(sec_fmt))
    axs[1, 1].set_ylabel("p99, секунд (лог. шкала)")
    axs[1, 1].set_xlabel("обработано, сообщений/с")
    for ax in (axs[0, 0], axs[0, 1], axs[1, 0]):
        ax.set_xlabel("Предложенная нагрузка, сообщений/с")
    for ax in axs.flat:
        ax.xaxis.set_major_formatter(FuncFormatter(n_fmt))
        clean_axes(ax, grid_x=True)
    axs[0, 0].yaxis.set_major_formatter(FuncFormatter(n_fmt))
    axs[0, 0].legend(loc="upper left")
    fig.subplots_adjust(left=0.08, right=0.985, top=0.94, bottom=0.08, wspace=0.24, hspace=0.34)
    return save(fig, out, "fig4_batching.png")


def fig_bottleneck(R, out):
    e5 = R["e5"]
    runs = e5["runs"]

    def name(r):
        b, st, v = r["batch_size_cfg"], r["storage_cfg"], r["validate_cfg"]
        s = f"пакет {b}"
        if st == "null":
            s += ", без записи в БД"
        if not v:
            s += ", без проверки схемы"
        return s

    labels = [name(r) for r in runs]
    y = np.arange(len(runs))[::-1]
    fig, axs = plt.subplots(1, 2, figsize=(11.8, 4.6), sharey=True)
    ax = axs[0]
    thr = [r["throughput"] for r in runs]
    ax.barh(y, thr, height=0.6, color=CAT[0], edgecolor=SURFACE)
    for yy, v in zip(y, thr):
        ax.text(v + max(thr) * 0.012, yy, n_fmt(v), va="center", fontsize=9)
    ax.set_xlim(0, max(thr) * 1.15)
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlabel(f"обработано при нагрузке {n_fmt(e5['load'])} сообщ./с, сообщений/с")
    ax.set_title("A. Пропускная способность")
    ax.xaxis.set_major_formatter(FuncFormatter(n_fmt))
    ax.grid(axis="x", color=GRID, lw=0.7)
    ax.grid(axis="y", visible=False)
    clean_axes(ax)

    ax = axs[1]
    h = 0.24
    for j, (k, c, lab) in enumerate((("mqtt_read", CAT[0], "приём"), ("process", CAT[1], "обработка (JSON, схема)"),
                                      ("storage", CAT[2], "запись в SQLite"))):
        v = np.array([r["busy"][k] * 100 for r in runs])
        ax.barh(y + (1 - j) * h, v, height=h * 0.92, color=c, edgecolor=SURFACE, linewidth=0.8, label=lab)
        for yy, vv in zip(y + (1 - j) * h, v):
            ax.text(vv + 1.0, yy, f"{vv:.0f}", va="center", fontsize=8)
    ax.set_xlim(0, 100)
    ax.set_xlabel("% времени, когда стадия занята")
    ax.set_title("B. Занятость стадий")
    h_, l_ = ax.get_legend_handles_labels()
    fig.legend(h_, l_, loc="lower center", ncol=3, bbox_to_anchor=(0.72, 0.0))
    ax.grid(axis="x", color=GRID, lw=0.7)
    ax.grid(axis="y", visible=False)
    ax.tick_params(axis="y", length=0)
    clean_axes(ax)
    fig.subplots_adjust(left=0.22, right=0.985, top=0.93, bottom=0.2, wspace=0.06)
    return save(fig, out, "fig5_bottleneck.png")


FIGS = [("fig_architecture", None), ("fig_load", "e1"), ("fig_saturation", "e2"), ("fig_overload", "e3"),
        ("fig_batching", "e4"), ("fig_bottleneck", "e5")]


def make_all(R, out):
    for name, need in FIGS:
        if need and need not in R:
            continue
        p = globals()[name](R, out)
        print(f"    {os.path.relpath(p, out)}")


if __name__ == "__main__":
    import sys
    here = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")
    R = json.load(open(os.path.join(here, "results.json"), encoding="utf-8"))
    make_all(R, here)
