from asyncio import open_connection
from typing import Optional

import asyncvnc

from PIL import Image


class AsyncVNCClient:

    def __init__(self, ip: str, password: str, port: int = 5900):
        self.ip = ip
        self.port = port
        self.password = password
        self._client: Optional[asyncvnc.Client] = None

    @property
    def is_closed(self):
        return self._client.writer.is_closing()

    @property
    def client(self):
        return self._client

    async def close(self):
        self._client.writer.close()
        await self._client.writer.wait_closed()

    async def connect(self) -> asyncvnc.Client:
        reader, writer = await open_connection(self.ip, self.port)
        return await asyncvnc.Client.create(reader, writer, None, self.password, None)

    async def __aenter__(self):
        try:
            self._client = await self.connect()
        except PermissionError:
            raise PermissionError("VNC密码错误，请重试！")
        except Exception:
            raise
        else:
            return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client is not None:
            await self.close()

    async def screenshot(self, filepath: str):
        pixels = await self._client.screenshot()
        image = Image.fromarray(pixels)
        image.save(f"{filepath}")

