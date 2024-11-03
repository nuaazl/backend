import asyncio
import traceback
from typing import Optional, Union, Callable, Any


class PasswordError(Exception):
    ...


class AuthError(Exception):
    ...


class AsyncSession:
    def __init__(
            self, *, ip: str, user: str, password: str,
            session: Any, heart_beat_timeout: Union[float, int] = 15,
            stop_callback: Optional[Callable] = None,
    ):
        """
        :param ip:
        :param user:
        :param password:
        :param session: ClientSession | 其他
        :param heart_beat_timeout: 心跳时间
        :param stop_callback:
        """
        self.ip = ip
        self.user = user
        self.password = password
        self.__session = session
        self.status = False
        self.heart_beat_timeout = heart_beat_timeout
        self.async_task: Optional[asyncio.Task] = None
        self.wrong_password: bool = False
        self.closed = False
        self.stop_callback = stop_callback

    def add_stop_callback(self, callback: Callable = None):
        if self.async_task and callback:
            self.async_task.add_done_callback(callback)

    def __str__(self):
        return f'AsyncSession--{self.ip}:{self.user}:{self.password}'

    def __repr__(self):
        return self.__str__()

    async def __aenter__(self):
        try:
            await self.update_session()
            self.async_task = asyncio.create_task(self.heart_beat())
            if self.stop_callback:
                self.async_task.add_done_callback(self.stop_callback)
            return self
        except Exception:
            await self.close()
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        if exc_val:
            raise Exception(exc_type, exc_val, exc_tb)

    @property
    def session(self):
        return self.__session

    async def update_session(self):
        raise NotImplementedError

    async def heart(self):
        """
        Must raise exception when connection lost or auth failed
        """
        raise NotImplementedError

    async def logout(self):
        raise NotImplementedError

    async def heart_beat(self):
        while True:
            if self.closed:
                self.status = False
                break
            try:
                await self.heart()
                self.status = True
            except Exception:
                self.status = False
                try:
                    await self.update_session()
                    self.status = True
                except PasswordError:
                    self.wrong_password = True
                    break
                except Exception:
                    print("刷新session失败....")
            finally:
                await asyncio.sleep(self.heart_beat_timeout)

    async def close(self):
        self.closed = True
        if self.async_task is not None:
            try:
                self.async_task.cancel()
            except Exception:
                print(traceback.format_exc())
                ...
        if self.status:
            try:
                await self.logout()
            except Exception:
                print(traceback.format_exc())
                ...
        await self.__session.close()
