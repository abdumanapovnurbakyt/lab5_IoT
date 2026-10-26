import asyncio
import logging
import select
import time

import paho.mqtt.client as mqtt

log = logging.getLogger("mqtt")


class AsyncMQTT:
    def __init__(self, host, port, client_id, topic=None, qos=1, on_message=None, keepalive=60, burst=500):
        self.host, self.port, self.topic, self.qos, self.keepalive = host, port, topic, qos, keepalive
        self.burst = burst         
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=True)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_socket_open = self._on_socket_open
        self.client.on_socket_close = self._on_socket_close
        self.client.on_socket_register_write = self._on_register_write
        self.client.on_socket_unregister_write = self._on_unregister_write
        if on_message:
            self.client.on_message = on_message
        self.loop = None
        self._sock = None
        self.paused = False
        self.connected = False
        self.read_seconds = 0.0         
        self._misc = None
        self._closing = False
        self._reconnect = None

    # ---- запуск / остановка
    async def start(self):
        self.loop = asyncio.get_running_loop()
        await self._connect_with_retry()

    async def _connect_with_retry(self):
        delay = 0.5
        while not self._closing:
            try:
                self.client.connect(self.host, self.port, self.keepalive)
                return
            except OSError as e:
                log.warning("брокер %s:%s недоступен (%s), повтор через %.1f с", self.host, self.port, e, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5.0)

    async def restart(self):
        self.paused = False
        try:
            self.client.disconnect()
        except Exception:                                        
            pass
        await asyncio.sleep(0.2)
        await self._connect_with_retry()

    async def stop(self):
        self._closing = True
        try:
            self.client.disconnect()
        except Exception:                                       
            pass

    def set_qos(self, qos):
        if qos == self.qos:
            return
        self.qos = qos
        if self.topic and self.connected:
            self.client.unsubscribe(self.topic)
            self.client.subscribe(self.topic, qos=qos)

    def pause(self):
        if self.paused or self._sock is None:
            return False
        self.loop.remove_reader(self._sock)
        self.paused = True
        return True

    def resume(self):
        if not self.paused:
            return False
        self.paused = False
        if self._sock is not None:
            self.loop.add_reader(self._sock, self._read)
        return True

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            log.error("подключение отклонено: %s", reason_code)
            return
        self.connected = True
        log.info("подключено к %s:%s", self.host, self.port)
        if self.topic:
            client.subscribe(self.topic, qos=self.qos)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connected = False
        if self._closing:
            return
        log.warning("соединение потеряно (%s), переподключаюсь", reason_code)
        if self.loop is not None and (self._reconnect is None or self._reconnect.done()):
            self._reconnect = self.loop.create_task(self._connect_with_retry())

    def _on_socket_open(self, client, userdata, sock):
        self._sock = sock
        if not self.paused:
            self.loop.add_reader(sock, self._read)
        if self._misc is None or self._misc.done():
            self._misc = self.loop.create_task(self._misc_loop())

    def _on_socket_close(self, client, userdata, sock):
        try:
            self.loop.remove_reader(sock)
            self.loop.remove_writer(sock)
        except Exception:                                         
            pass
        self._sock = None

    def _on_register_write(self, client, userdata, sock):
        self.loop.add_writer(sock, self._write)

    def _on_unregister_write(self, client, userdata, sock):
        self.loop.remove_writer(sock)

    def _readable(self):
        poll = select.poll()
        poll.register(self._sock, select.POLLIN)
        return bool(poll.poll(0))

    def _read(self):
        t = time.perf_counter()
        for _ in range(self.burst):
            self.client.loop_read()
            if self._sock is None or self.paused or not self._readable():
                break
        self.read_seconds += time.perf_counter() - t

    def _write(self):
        self.client.loop_write()

    async def _misc_loop(self):
        while True:
            if self.client.loop_misc() != mqtt.MQTT_ERR_SUCCESS:
                return
            await asyncio.sleep(1.0)
