# 用来管理连接


import asyncio
import time
import traceback
from typing import Dict, Any, Union, Callable, Optional, Literal, cast

from utils.auth.base import AsyncSession, PasswordError
from utils.auth.example_session import ExampleSession
from utils.ssh import NoFTPAsyncSSH
from utils.vnc import AsyncVNCClient

SESSION_KIND = Literal["EXAMPLE",]
SESSION_CLASS = {
    "EXAMPLE": ExampleSession,
}


class Pydis:
    _lock = asyncio.Lock()
    _create_lock = asyncio.Lock()
    clear_task = None

    _object_map: Dict[str, Any] = dict()
    _object_stop_map: Dict[str, Optional[Union[float, int]]] = dict()

    @classmethod
    async def init(cls):
        """如果清除任务不存在或者已经停止时，会启动，为了避免重复启动，需要lock"""
        if cls.clear_task_running():
            return
        async with cls._lock:
            if cls.clear_task_running():
                return
            try:
                _loop = asyncio.get_event_loop()
                inspect_task = _loop.create_task(cls.__close())
            except Exception:
                print('pydis init failed')
                raise Exception(f"pydis初始化失败：{traceback.format_exc()}")
            else:
                cls.clear_task = inspect_task

                def _clear(*args):
                    cls.clear_task = None
                cls.clear_task.add_done_callback(_clear)

    @classmethod
    def clear_task_running(cls):
        """检查定时清理任务是否存在，以及任务是否已经完成"""
        return cls.clear_task is not None and not cls.clear_task.done()

    @classmethod
    def exist(cls, key: str) -> bool:
        return cls._object_map.get(key) is not None

    @classmethod
    async def create_object(cls, _class: Callable, key: str, *args: Any, **kwargs: Any) -> Any:
        async with cls._create_lock:
            if key in cls._object_map:
                handler = cls._object_map[key]
                if hasattr(handler, "is_closing") and handler.is_closed:
                    await handler.close()
                    cls._object_map.pop(key)
                else:
                    return handler
            obj = await _class(*args, **kwargs).__aenter__()
            cls._object_map[key] = obj
            return obj

    @classmethod
    async def close_object(cls, key: str):
        if not cls.exist(key):
            return
        handler = cls._object_map.pop(key)
        await handler.close()

    @classmethod
    async def get_session(
            cls,
            ip: str,
            user: str,
            password: str,
            kind: SESSION_KIND = "EXAMPLE",
            connect_times: int = 1800,
            stop_callback: Optional[Callable] = None) -> AsyncSession:
        """
        获取连接的session
        """
        key = f"{ip}_{user}_{kind.lower()}"
        if key not in cls._object_map:
            try:
                handler = await cls.create_object(
                    SESSION_CLASS.get(kind), key, ip, user, password,
                    stop_callback=stop_callback
                )
            except TimeoutError:
                raise TimeoutError(f"连接{ip}超时，请检查对象是否在线")
        else:
            handler = cls._object_map[key]
            if handler.password != password and handler.status is True:
                raise PasswordError(f"{ip}用户名或者密码错误！")
            if handler.wrong_password:
                raise PasswordError(f"{ip}用户名或者密码错误！")
        cls._object_stop_map[key] = time.time() + connect_times
        await cls.init()
        return handler

    @classmethod
    async def get_vnc_client(
            cls,
            ip: str,
            password: str,
            port: int = 5900,
            connect_times: int = 600
    ) -> AsyncVNCClient:
        key = f"{ip}_vnc"
        if key not in cls._object_map or cls._object_map[key].is_closed is True:
            try:

                handler = await cls.create_object(
                    AsyncVNCClient, key, ip, port, password
                )
            except TimeoutError:
                raise TimeoutError(f"连接{ip}超时，请检查设备是否在线")
        else:
            handler = cls._object_map[key]
            if handler.password != password:
                raise PasswordError(f"{ip}VNC密码错误！")
        cls._object_stop_map[key] = time.time() + connect_times
        await cls.init()
        return handler

    @classmethod
    async def get_ssh_client(
            cls,
            ip: str,
            user: str,
            password: str,
            port: int = 22,
            connect_times: int = 600
    ):
        key = f"{ip}_{user}_ssh"
        if key not in cls._object_map or cls._object_map[key].is_closed:
            try:
                handler = await cls.create_object(
                    NoFTPAsyncSSH, key, ip, user, password, port=port
                )
            except TimeoutError:
                raise TimeoutError(f"连接{ip}超时，请检查对象是否在线")
        else:
            handler: NoFTPAsyncSSH = cls._object_map[key]
            if handler.password != password:
                raise PasswordError(f"{ip}用户名或者密码错误！")
        cls._object_stop_map[key] = time.time() + connect_times
        await cls.init()
        return handler

    @classmethod
    async def __close(cls) -> None:
        while True:
            now = time.time()
            try:
                for key in cls._object_stop_map:
                    value = cls._object_stop_map[key]
                    if value is not None and now > value:
                        await cls._object_map[key].close()
                        cls._object_map.pop(key)
                        cls._object_stop_map[key] = None
            except Exception:
                print(traceback.format_exc())
            if len(cls._object_map) == 0:
                cls._object_stop_map.clear()
                break
            await asyncio.sleep(300)
