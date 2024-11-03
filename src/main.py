import asyncio

from contextlib import asynccontextmanager
from typing import cast

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.core import AgnosticCollection
from pymongo.errors import OperationFailure

from router import router
from config import Config
from utils.mongo_client import AsyncMongoClient
from utils.scheduler import Scheduler

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    ...

@asynccontextmanager
async def lifespan(application: FastAPI):
    AsyncMongoClient.start(Config.MONGO_STR)
    AsyncMongoClient.switch_db(Config.MONGO_DATABASE)
    config_db = cast(AgnosticCollection, AsyncMongoClient["job_lock"])
    try:
        await config_db.create_index(
            [("ttl_time", 1)], expireAfterSeconds=30
        )
    except OperationFailure as e:
        print("创建索引失败：", str(e))

    async_scheduler = AsyncIOScheduler()
    Scheduler.init("async", async_scheduler)
    Scheduler.start()
    yield
    AsyncMongoClient.close()


app = FastAPI(
    title="113TestWeb",
    description="努力找工作！",
    version="0.0.1",
    lifespan=lifespan
)

origins = [
    "http://127.0.0.1",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router, prefix="")

@app.get("/status")
async def service_is_healthy():
    await asyncio.sleep(0.1)
    return {
        "status": "ok"
    }
