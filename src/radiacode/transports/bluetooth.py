import asyncio
import platform
import struct
import time

from radiacode.bytes_buffer import BytesBuffer
from ..logger import logger
from bleak import BleakClient, BleakError, BleakGATTCharacteristic

SERVICE_UUID = "e63215e5-7003-49d8-96b0-b024798fb901"
WRITE_UUID = "e63215e6-7003-49d8-96b0-b024798fb901"
NOTIFY_UUID = "e63215e7-7003-49d8-96b0-b024798fb901"

class DeviceNotFound(Exception):
    pass

class ConnectionClosed(Exception):
    pass

class Bluetooth:
    """
    Bleak-based replacement for old bluepy Bluetooth connection.
    Provides synchronous-style execute() interface for RadiaCode.
    """
    def __init__(self, mac, poll_interval: float = 0.01):
        
        self._mac = mac
        self._poll_interval = poll_interval
        self._resp_buffer = b""
        self._resp_size = 0
        self._response_future: asyncio.Future | None = None
        self._closing = False

        self._loop = asyncio.get_event_loop()
        self._client = BleakClient(mac)
        self._rx_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._connect_task = self._loop.create_task(self._connect())

        # Wait for connection to complete
        self._loop.run_until_complete(self._connect_task)
        
        self.connect_attempts = 5
    async def _connect(self):
        logger.info(f"📶 Connecting to {self._mac}")
        connect_attempts = getattr(self, "connect_attempts", 5)
        for i in range(1, connect_attempts + 1):
            try:
                await self._client.connect()
                if not self._client.is_connected:
                    raise DeviceNotFound(f"Failed to connect to {self._mac}")

                await self._client.start_notify(NOTIFY_UUID, self._handle_notification)
                logger.info("✅ Client started successfully")
                break
            except Exception as e:
                logger.warning(f"❌ Connection attempt {i}/{connect_attempts} failed! Retrying in 3s...")
                asyncio.sleep(3)

    def _handle_notification(self, _: BleakGATTCharacteristic, data: bytearray):
        """
        Push notifications handler — accumulates response for synchronous-style execute().
        """
        if self._resp_size == 0:
            if len(data) < 4:
                # malformed packet
                return
            self._resp_size = 4 + struct.unpack("<i", data[:4])[0]
            self._resp_buffer = data[4:]
        else:
            self._resp_buffer += data
        self._resp_size -= len(data)
        if self._resp_size <= 0 and self._response_future and not self._response_future.done():
            self._response_future.set_result(self._resp_buffer)
            self._resp_buffer = b""
            self._resp_size = 0

    def execute(self, req: bytes, timeout: float = 10.0) -> BytesBuffer:
        """
        Send a request to the device and wait for the full response.
        Mimics the old synchronous execute() method.
        """
        if self._closing:
            raise ConnectionClosed("Connection is closing")

        async def _send_and_wait():
            self._response_future = asyncio.Future()

            # Chunk the request (max 18 bytes per BLE write)
            try:
                for pos in range(0, len(req), 18):
                    chunk = req[pos : min(pos + 18, len(req))]
                    await self._client.write_gatt_char(WRITE_UUID, chunk)
            except BleakError:
                self._response_future = None
                return

            try:
                return await asyncio.wait_for(self._response_future, timeout)
            except asyncio.TimeoutError:
                raise TimeoutError("Response timeout")
            finally:
                self._response_future = None

        # Run the coroutine in the event loop and wait synchronously
        response_data = self._loop.run_until_complete(_send_and_wait())
        return BytesBuffer(response_data)

    def close(self):
        """Disconnect from the device and cleanup."""
        self._closing = True

        async def _cleanup():
            try:
                if self._client.is_connected:
                    await self._client.stop_notify(NOTIFY_UUID)
                    await self._client.disconnect()
            except:  # noqa: E722
                pass

        self._loop.run_until_complete(_cleanup())