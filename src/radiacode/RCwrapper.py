import asyncio
import time
from dataclasses import dataclass

import numpy as np

from .types import RealTimeData, RawData, RareData
from .radiacode import RadiaCode
from .logger import logger

@dataclass(frozen=True)
class CurrentValuesPackage:
    CPS: float
    DR: float
    timestamp: float

@dataclass(frozen=True)
class StatusPackage:
    battery: int
    temperature: float
    charging: bool
    acc_dose: float
    dose_acc_time: float
    timestamp: float

@dataclass(frozen=True)
class SpectrumResult:
    spectrum: np.ndarray
    counts: int
    uptime: float
    calib_coeff: list
    timestamp: float

class RadiacodeAsync:
    def __init__(self, address, usb = False):
        self.address = address
        self._usb = usb
        
        try:
            self.name = address.name
        except AttributeError:
            self.name = str(address)
            
        self._lock = asyncio.Lock()
        
        self._latest_cps = None
        self._latest_spectrum = None
        self._latest_status = None

        self._lock = asyncio.Lock()
        self._poll_task = None
        self._pending_request = False
        
        self._stopped = False
        self._running = False
        
        self.update_task = None
        
    

    # ---------------- PUBLIC API ----------------
    @property
    def LatestRealTimeData(self):
        if self.update_task is None or self.update_task.done():
            self.update_task = asyncio.create_task(self._update_current())
        return self._latest_cps

    @property
    def LatestSpectrum(self):
        if self.update_task is None or self.update_task.done():
            self.update_task = asyncio.create_task(self._update_current(True))
        return self._latest_spectrum 
    
    @property
    def LastestStatus(self):
        return self._latest_status
        
    async def start(self):
        if self._usb:
            self.client = await asyncio.to_thread(RadiaCode, None, self.address)
            logger.info(f"Radiacode {self.name} successfully connected by USB")
        else:
            self.client = RadiaCode(self.address)
            logger.info(f"Radiacode {self.name} successfully connected by BLE")
        self._running = True
        
    async def stop(self):
        self._stopped = True
        self.client.stop()
        if self.update_task is not None and not self.update_task.done():
            await self.update_task.cancel()
        
    def reset(self):
        self.client.spectrum_reset()

        
    # ---------------- INTERNAL ----------------
    async def _update_current(self, get_spectrum = False):
        if self._pending_request:
            return  # don't start multiple simultaneous requests
        self._pending_request = True
        try:
            loop = asyncio.get_running_loop()
            data = await loop.run_in_executor(None, self.client.data_buf)
            self.decode_cps_packet(data)
            if get_spectrum:
                spectrum = await loop.run_in_executor(None, self.client.spectrum)
                
                self._latest_spectrum = SpectrumResult(np.array(spectrum.counts), sum(spectrum.counts), spectrum.duration.total_seconds(), 
                                                    [spectrum.a2, spectrum.a1, spectrum.a0], time.time())
        except Exception as e:
            logger.error(f"Update task failed with {e}")  # could log error
        finally:
            self._pending_request = False
            
    def decode_cps_packet(self, data: list):
        for packet in data:
            if isinstance(packet, RealTimeData):
                self._latest_cps = CurrentValuesPackage(packet.count_rate, packet.dose_rate * 1e4, packet.dt.timestamp())
            elif isinstance(packet, RareData):
                self._latest_status = StatusPackage(packet.charge_level, packet.charge_level, False, packet.dose, packet.duration, packet.dt.timestamp())