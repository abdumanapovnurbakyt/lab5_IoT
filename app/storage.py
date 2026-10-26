import os
import sqlite3
import time

COLUMNS = ("device_id", "seq", "ts", "recv_ts", "temperature", "humidity", "battery_mv", "site", "device_type", "run_id")


class SqliteStorage:
    def __init__(self, path, sync="FULL"):
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute(f"PRAGMA synchronous={sync}")
        self.db.execute(f"CREATE TABLE IF NOT EXISTS telemetry ({', '.join(COLUMNS)})")
        self._sql = f"INSERT INTO telemetry VALUES ({', '.join('?' * len(COLUMNS))})"

    def write(self, rows):
        t = time.perf_counter()
        self.db.execute("BEGIN")
        self.db.executemany(self._sql, rows)
        self.db.execute("COMMIT")
        return time.perf_counter() - t

    def verify(self, run_id):
        r = self.db.execute("SELECT COUNT(*), COUNT(DISTINCT device_id || ':' || seq) FROM telemetry WHERE run_id=?",
                            (run_id,)).fetchone()
        return int(r[0]), int(r[1])

    def clear(self):
        self.db.execute("DELETE FROM telemetry")

    def close(self):
        self.db.close()


class NullStorage:

    def write(self, rows):
        return 0.0

    def verify(self, run_id):
        return 0, 0

    def clear(self):
        pass

    def close(self):
        pass
