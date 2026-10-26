import os
from dataclasses import asdict, dataclass

POLICIES = ("none", "pause", "shed")
STORAGES = ("sqlite", "null")


def _env(name, default, cast=str):
    v = os.environ.get(name)
    return default if v is None or v == "" else cast(v)


def _bool(v):
    return str(v).strip().lower() in ("1", "true", "yes", "on")


@dataclass
class RuntimeConfig:
    """То, что меняется между прогонами без перезапуска сервиса."""
    batch_size: int = 500          # max_batch_size
    max_delay_s: float = 2.0       # max_delay: сколько ждать заполнения пакета, считая от самого старого сообщения
    policy: str = "none"           # none - без ограничений; pause - backpressure (перестать читать); shed - отбрасывать
    max_inflight: int = 5000       # сколько сообщений может быть принято, но ещё не записано
    resume_ratio: float = 0.5      # чтение возобновляется, когда inflight упал до resume_ratio * max_inflight
    storage: str = "sqlite"        # sqlite - реальная запись; null - без записи (диагностика узкого места)
    log_batches: bool = True       # писать в лог каждый пакет (реальный размер, причину сброса)
    validate_schema: bool = True   # проверять JSON Schema (False - только для диагностики узкого места)
    qos: int = 0                   # QoS подписки ingest-сервиса (0 - без подтверждений, 1 - с окном inflight у брокера)

    def validate(self):
        if self.batch_size < 1:
            raise ValueError("batch_size должен быть >= 1")
        if self.max_delay_s <= 0:
            raise ValueError("max_delay_s должен быть > 0")
        if self.policy not in POLICIES:
            raise ValueError(f"policy должен быть одним из {POLICIES}")
        if self.storage not in STORAGES:
            raise ValueError(f"storage должен быть одним из {STORAGES}")
        if self.qos not in (0, 1):
            raise ValueError("qos должен быть 0 или 1")
        if self.max_inflight < 1:
            raise ValueError("max_inflight должен быть >= 1")
        if not 0.0 < self.resume_ratio < 1.0:
            raise ValueError("resume_ratio должен быть между 0 и 1")
        return self

    def as_dict(self):
        return asdict(self)


@dataclass
class Settings:
    mqtt_host: str
    mqtt_port: int
    topic: str
    db_path: str
    schema_path: str
    sqlite_sync: str
    runtime: RuntimeConfig

    @classmethod
    def from_env(cls):
        rt = RuntimeConfig(
            batch_size=_env("BATCH_SIZE", 500, int),
            max_delay_s=_env("MAX_DELAY_S", 2.0, float),
            policy=_env("POLICY", "none"),
            max_inflight=_env("MAX_INFLIGHT", 5000, int),
            resume_ratio=_env("RESUME_RATIO", 0.5, float),
            storage=_env("STORAGE", "sqlite"),
            log_batches=_env("LOG_BATCHES", True, _bool),
            validate_schema=_env("VALIDATE", True, _bool),
            qos=_env("MQTT_QOS", 0, int),
        ).validate()
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return cls(
            mqtt_host=_env("MQTT_HOST", "mosquitto"),
            mqtt_port=_env("MQTT_PORT", 1883, int),
            topic=_env("MQTT_TOPIC", "telemetry/#"),
            db_path=_env("DB_PATH", "/data/telemetry.db"),
            schema_path=_env("SCHEMA_PATH", os.path.join(here, "schemas", "telemetry.schema.json")),
            sqlite_sync=_env("SQLITE_SYNC", "FULL"),
            runtime=rt,
        )
