import asyncio

from .base import AsyncSession


class ExampleSession(AsyncSession):
    """
    session的例子，heart_beat_count = 50的时候心跳挂掉1次
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.heart_beat_count = 0  #

    async def update_session(self):
        await asyncio.sleep(1)

    async def heart(self):
        await asyncio.sleep(0.5)
        self.heart_beat_count += 1
        if self.heart_beat_count == 50:
            self.heart_beat_count = 0
            raise TimeoutError("假心跳异常")

    async def logout(self):
        await asyncio.sleep(0.5)
        print(f"{self.ip}注销成功！")
