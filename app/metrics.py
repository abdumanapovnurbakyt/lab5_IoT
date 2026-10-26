from bisect import bisect_left

from prometheus_client import CollectorRegistry, ProcessCollector
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, HistogramMetricFamily

LATENCY_BOUNDS = (0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0,
                  5.0, 10.0, 20.0, 30.0, 60.0, 120.0)
BATCH_BOUNDS = (1, 2, 5, 10, 20, 50, 100, 200, 300, 400, 500, 1000)
WRITE_BOUNDS = (0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0)


class Hist:
    """Гистограмма с накопительными корзинами (le), как в Prometheus."""
    __slots__ = ("bounds", "counts", "sum", "count")

    def __init__(self, bounds):
        self.bounds = tuple(bounds)
        self.counts = [0] * (len(self.bounds) + 1)
        self.sum = 0.0
        self.count = 0

    def observe(self, v):
        self.counts[bisect_left(self.bounds, v)] += 1
        self.sum += v
        self.count += 1

    def observe_many(self, values):
        counts, bounds = self.counts, self.bounds
        for v in values:
            counts[bisect_left(bounds, v)] += 1
        self.sum += sum(values)
        self.count += len(values)

    def buckets(self):
        out, acc = [], 0
        for b, c in zip(self.bounds, self.counts):
            acc += c
            out.append((repr(float(b)), acc))
        out.append(("+Inf", acc + self.counts[-1]))
        return out


class Metrics:
    def __init__(self):
        self.received = 0                
        self.invalid = {"json": 0, "schema": 0}
        self.processed = 0               
        self.stored = 0
        self.dropped = {"shed": 0, "storage_error": 0, "discarded": 0}
        self.flush = {"size": 0, "timeout": 0}
        self.mqtt_read_s = 0.0          
        self.process_s = 0.0        
        self.storage_s = 0.0              
        self.pause_s = 0.0             
        self.pauses = 0
        self.lag_max = 0.0             
        self.batch = Hist(BATCH_BOUNDS)
        self.proc = Hist(LATENCY_BOUNDS) 
        self.e2e = Hist(LATENCY_BOUNDS)   
        self.write = Hist(WRITE_BOUNDS) 


class PipelineCollector:
    def __init__(self, pipeline):
        self.p = pipeline

    def collect(self):
        p, m = self.p, self.p.m
        p.sync_read()
        yield CounterMetricFamily("messages_received", "Сообщений получено из MQTT", value=m.received)
        c = CounterMetricFamily("messages_invalid", "Сообщений отклонено валидацией", labels=["reason"])
        for k, v in m.invalid.items():
            c.add_metric([k], v)
        yield c
        yield CounterMetricFamily("messages_processed", "Сообщений обработано (валидация и обогащение пройдены)",
                                  value=m.processed)
        yield CounterMetricFamily("messages_stored", "Сообщений записано в хранилище", value=m.stored)
        c = CounterMetricFamily("messages_dropped", "Сообщений отброшено сервисом", labels=["reason"])
        for k, v in m.dropped.items():
            c.add_metric([k], v)
        yield c
        c = CounterMetricFamily("batches_flushed", "Сброшено пакетов", labels=["reason"])
        for k, v in m.flush.items():
            c.add_metric([k], v)
        yield c
        yield CounterMetricFamily("mqtt_read_seconds", "Время стадии приёма (чтение сокета, разбор MQTT)", value=m.mqtt_read_s)
        yield CounterMetricFamily("process_seconds", "Время стадии обработки (JSON, схема, обогащение)", value=m.process_s)
        yield CounterMetricFamily("storage_write_seconds", "Время записи в хранилище", value=m.storage_s)
        yield CounterMetricFamily("backpressure_pause_seconds", "Время приостановки чтения (backpressure)",
                                  value=m.pause_s + p.current_pause_s())
        yield CounterMetricFamily("backpressure_pauses", "Сколько раз чтение приостанавливалось", value=m.pauses)

        yield GaugeMetricFamily("queue_depth", "Сообщений в очереди сервиса (приёмная очередь + буфер пакета)",
                                value=p.queue_depth)
        yield GaugeMetricFamily("raw_queue_depth", "Сообщений в приёмной очереди (ещё не обработаны)", value=len(p.raw))
        yield GaugeMetricFamily("inflight_messages", "Принято, но ещё не записано", value=p.inflight)
        yield GaugeMetricFamily("backpressure_paused", "1, если чтение из MQTT приостановлено",
                                value=1 if p.paused else 0)
        yield GaugeMetricFamily("mqtt_connected", "1, если есть соединение с брокером",
                                value=1 if p.mqtt_connected() else 0)
        yield GaugeMetricFamily("event_loop_lag_seconds", "Макс. задержка цикла событий с прошлого запроса",
                                value=m.lag_max)
        m.lag_max = 0.0
        g = GaugeMetricFamily("ingest_config_info", "Текущие правила батчинга и перегрузки (значение всегда 1)",
                              labels=["batch_size", "max_delay_s", "policy", "max_inflight", "storage"])
        cfg = p.cfg
        g.add_metric([str(cfg.batch_size), str(cfg.max_delay_s), cfg.policy, str(cfg.max_inflight), cfg.storage], 1)
        yield g

        for name, doc, h in (("batch_size", "Реальный размер сброшенного пакета", m.batch),
                             ("processing_latency_seconds", "Получено ingest-ом -> записано", m.proc),
                             ("e2e_latency_seconds", "Создано устройством -> записано", m.e2e),
                             ("storage_commit_seconds", "Длительность записи пакета", m.write)):
            yield HistogramMetricFamily(name, doc, buckets=h.buckets(), sum_value=h.sum)


def make_registry(pipeline):
    reg = CollectorRegistry()
    reg.register(PipelineCollector(pipeline))
    ProcessCollector(registry=reg)       
    return reg
