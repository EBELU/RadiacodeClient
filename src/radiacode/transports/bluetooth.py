import asyncio
import threading
import struct
from bleak import BleakClient, BleakError, BleakGATTCharacteristic

from ..bytes_buffer import BytesBuffer
from ..logger import logger

SERVICE_UUID = "e63215e5-7003-49d8-96b0-b024798fb901"
WRITE_UUID   = "e63215e6-7003-49d8-96b0-b024798fb901"
NOTIFY_UUID  = "e63215e7-7003-49d8-96b0-b024798fb901"


class DeviceNotFound(Exception):
    pass


class ConnectionClosed(Exception):
    pass


class Bluetooth:
    """
    Sync facade over async Bleak transport.
    Runs its own asyncio loop in a dedicated thread.
    """

    def __init__(self, mac: str, timeout: float = 10.0):
        self._mac = mac
        self._timeout = timeout
        self._closing = False

        self._resp_buffer = b""
        self._resp_size = 0
        self._response_future: asyncio.Future | None = None
        
        self.connection_attemps = 5

        # Dedicated event loop + thread
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True
        )
        self._thread.start()

        # Create client inside BLE loop
        self._client: BleakClient | None = None

        # Block until connected
        fut = asyncio.run_coroutine_threadsafe(
            self._async_init(),
            self._loop
        )
        
        try:
            fut.result(timeout=10)
        except Exception:
            self.stop()
            raise

    # ------------------------------------------------
    # Thread loop runner
    # ------------------------------------------------

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    # ------------------------------------------------
    # Async initialization
    # ------------------------------------------------

    async def _async_init(self):
        self._client = BleakClient(self._mac)

        logger.info(f"📶 Connecting to {self._mac}")
        for i in range(self.connection_attemps):
            try:
                await self._client.connect()
                break
            except Exception as e:
                if i + 1 != self.connection_attemps:
                    logger.warning(f"Connection attempt {i + 1}/{self.connection_attemps} failed, retrying in 3s ...")
                    await asyncio.sleep(3)
                else:
                    logger.error(f"Connection to {self._mac} failed!")
                    self.stop()

        if not self._client.is_connected:
            raise DeviceNotFound(f"Failed to connect to {self._mac}")

        await self._client.start_notify(
            NOTIFY_UUID,
            self._handle_notification
        )

        logger.info("✅ BLE connected and notifications started")

    # ------------------------------------------------
    # Notification handler (runs in BLE loop thread)
    # ------------------------------------------------

    def _handle_notification(self, _: BleakGATTCharacteristic, data: bytearray):
        if self._resp_size == 0:
            if len(data) < 4:
                return
            self._resp_size = 4 + struct.unpack("<i", data[:4])[0]
            self._resp_buffer = data[4:]
        else:
            self._resp_buffer += data

        self._resp_size -= len(data)

        if self._resp_size <= 0 and self._response_future:
            if not self._response_future.done():
                self._response_future.set_result(self._resp_buffer)

            self._resp_buffer = b""
            self._resp_size = 0

    # ------------------------------------------------
    # Public synchronous execute()
    # ------------------------------------------------

    def execute(self, req: bytes, timeout: float | None = None) -> BytesBuffer:
        if self._closing:
            raise ConnectionClosed("Connection is closing")

        timeout = timeout or self._timeout

        future = asyncio.run_coroutine_threadsafe(
            self._async_execute(req, timeout),
            self._loop
        )

        result = future.result(timeout + 1)
        return BytesBuffer(result)

    # ------------------------------------------------
    # Async execute (runs in BLE thread)
    # ------------------------------------------------

    async def _async_execute(self, req: bytes, timeout: float):
        if not self._client or not self._client.is_connected:
            raise ConnectionClosed("BLE not connected")

        loop = asyncio.get_running_loop()
        self._response_future = loop.create_future()

        # BLE MTU chunking (18 bytes safe)
        for pos in range(0, len(req), 18):
            chunk = req[pos:pos + 18]
            await self._client.write_gatt_char(WRITE_UUID, chunk)

        try:
            return await asyncio.wait_for(self._response_future, timeout)
        finally:
            self._response_future = None

    # ------------------------------------------------
    # Cleanup
    # ------------------------------------------------

    def stop(self):
        self._closing = True

        async def _cleanup():
            if self._client and self._client.is_connected:
                await self._client.stop_notify(NOTIFY_UUID)
                await self._client.disconnect()

        fut = asyncio.run_coroutine_threadsafe(_cleanup(), self._loop)
        fut.result()

        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()