import asyncio
from asyncio import AbstractEventLoop
from typing import Optional, Dict

from motor.core import AgnosticDatabase, AgnosticCollection
from motor.motor_asyncio import AsyncIOMotorClient


class __MotorMongodbMeta(type):
    _client = None
    _db: AgnosticDatabase = None
    motor_databases: Dict[str, AgnosticDatabase] = dict()

    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __getitem__(self, name: str) -> AgnosticCollection:
        """
        :param name:
        :return:
        """
        if self._db is None:
            raise Exception("db未初始化！")
        return self._db[name]


class AsyncMongoClient(metaclass=__MotorMongodbMeta):

    @classmethod
    def start(cls, uri: str, lp: Optional[AbstractEventLoop] = None):
        _loop = lp or asyncio.get_event_loop()
        cls._client = AsyncIOMotorClient(uri, io_loop=_loop)

    @classmethod
    def close(cls):
        try:
            cls._client.close()
        except Exception:
            pass

    @classmethod
    def switch_db(cls, db_name: str):
        if (db := cls.motor_databases.get(db_name)) is None:
            db = cls._client[db_name]
            cls.motor_databases[db_name] = db
        cls._db = db

async def main():
    from config import Config
    AsyncMongoClient.start(Config.MONGO_STR)
    AsyncMongoClient.switch_db(Config.MONGO_DATABASE)
    col: AgnosticCollection = AsyncMongoClient["test"]
    print(type(col), isinstance(col, AgnosticCollection))
    res = await col.find_one({"a": 1})
    print(res, type(res))
    AsyncMongoClient.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
