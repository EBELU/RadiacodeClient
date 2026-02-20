import asyncio
import time
from dataclasses import dataclass

import numpy as np

from .types import RealTimeData, RawData, RareData
from radiacode import RadiaCode

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
    timestamp: float

@dataclass(frozen=True)
class SpectrumResult:
    spectrum: np.ndarray
    counts: int
    uptime: float
    calib_coeff: list
    timestamp: float

class RadiacodeAsync:
    def __init__(self, address):
        self.address = address
        self.client = RadiaCode(self.address)
        self._lock = asyncio.Lock()
        
        self._latest_cps = None
        self._latest_spectrum = None
        self._latest_status = None

        self._lock = asyncio.Lock()
        self._poll_task = None
        self._pending_request = False
        
        self.stopped = False
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
            self.update_task = asyncio.create_task(self._update_current())
        return self._latest_spectrum 
    
    @property
    def LastestStatus(self):
        return self._latest_status
        
    async def start(self):
        self.running = True
        
    async def stop(self):
        self.stopped = True
        
    def reset(self):
        self.client.spectrum_reset()

        
    # ---------------- INTERNAL ----------------
    async def _update_current(self):
        if self._pending_request:
            return  # don't start multiple simultaneous requests
        self._pending_request = True
        try:
            loop = asyncio.get_running_loop()
            data = await loop.run_in_executor(None, self.client.data_buf)
            spectrum = await loop.run_in_executor(None, self.client.spectrum)
            
            self.decode_cps_packet(data)
            
            self._latest_spectrum = SpectrumResult(np.array(spectrum.counts), sum(spectrum.counts), spectrum.duration.total_seconds(), 
                                                 [spectrum.a0, spectrum.a1, spectrum.a2], time.time())
        except Exception as e:
            print(str(e))  # could log error
        finally:
            self._pending_request = False
            
    def decode_cps_packet(self, data: list):
        for packet in data:
            if isinstance(packet, RealTimeData):
                self._latest_cps = CurrentValuesPackage(packet.count_rate, packet.dose_rate, packet.dt.timestamp())
