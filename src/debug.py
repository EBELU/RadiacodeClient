import asyncio
from radiacode import RadiaCode

from scanner import find_device_by_name

import asyncio

# rc = RadiaCode("52:43:09:90:06:52")

# async def poll(rc):
#     loop = asyncio.get_running_loop()
#     while True:
#         spec = await loop.run_in_executor(None, rc.spectrum)
#         buf = await loop.run_in_executor(None, rc.data_buf)
#         print(spec)
#         print(buf)
#         await asyncio.sleep(0.5)

# async def main():
#     # Start polling
#     asyncio.create_task(poll(rc))

#     # Keep the loop running forever
#     while True:
#         await asyncio.sleep(3600)

# # Run the event loop
# asyncio.run(main())

import asyncio
from radiacode import RadiaCode
from radiacode import RadiacodeAsync
from scanner import find_device_by_name

import logging

# Set global logging level to INFO
logging.basicConfig(level=logging.INFO)

rc = RadiacodeAsync("52:43:09:90:06:52")

async def poll(rc):
    loop = asyncio.get_running_loop()
    while True:
        print(rc.LatestRealTimeData)
        print("L", rc.LatestSpectrum)
        await asyncio.sleep(0.5)

async def main():
    # Start polling
    asyncio.create_task(poll(rc))
    await rc.start()

    # Keep the loop running forever
    while True:
        await asyncio.sleep(3600)

# Run the event loop
asyncio.run(main())