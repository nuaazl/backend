import asyncio
import functools
from typing import Optional, Literal, Callable, cast
from datetime import datetime, timedelta, UTC

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.events import JobEvent, JobSubmissionEvent, JobExecutionEvent, EVENT_ALL_JOBS_REMOVED, \
    EVENT_JOB_MODIFIED, EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_JOB_MAX_INSTANCES, EVENT_JOB_REMOVED, \
    EVENT_JOB_SUBMITTED, EVENT_JOB_EXECUTED, EVENT_JOB_ADDED
from motor.core import AgnosticCollection
from utils.mongo_client import AsyncMongoClient

SCHEDULER_KIND = Literal["unasync", "async"]


class DistributedLockAcquireError(Exception):
    ...


class _DistributedLockByMongodb:

    def __init__(self, key: str, ttl: Optional[datetime]):
        self.key = key
        self.ttl = ttl
        self.job_lock_db = cast(AgnosticCollection, AsyncMongoClient["job_lock"])

    async def __aenter__(self):
        doc = {"_id": self.key}
        if self.ttl is not None:
            doc["ttl_time"] = self.ttl
        try:
            await self.job_lock_db.insert_one(doc)
        except Exception:
            print(self.key, f" failed! {datetime.now()}")
            raise DistributedLockAcquireError("获取锁失败！")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self.job_lock_db.delete_one({"_id": self.key})
        except Exception as e:
            print(f"{e}, _DistributedLockByMongodb")


def job_lock(
        unique_key: str = "",
        expire_after_seconds: Optional[float] = None,
        expire_at: Optional[datetime] = None,
):
    def decorator(func):
        if not asyncio.iscoroutinefunction(func):
            raise TypeError(f"func must be a coroutine func, got {type(func)}")

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            run_times = kwargs.get("run_times")
            if run_times is not None and datetime.now() not in run_times:
                return
            key = f"{unique_key}_{func.__name__}_{'_'.join(filter(lambda x: isinstance(x, str), args))}"
            if expire_at is None and expire_after_seconds is None:
                ttl_time = datetime.now(UTC)
            elif expire_after_seconds:
                ttl_time = datetime.now(UTC) + timedelta(seconds=expire_after_seconds) - timedelta(seconds=30)
            else:
                ttl_time = expire_at
            async with _DistributedLockByMongodb(key=key, ttl=ttl_time):
                print(func.__name__, f" succeed! {datetime.now()}")
                return await func(*args, **kwargs)

        return wrapper

    return decorator


class Scheduler(object):
    async_scheduler: AsyncIOScheduler = None
    event_dispatch_dict = dict()

    @classmethod
    def init(cls, kind: SCHEDULER_KIND, scheduler: BaseScheduler):
        if kind == "async":
            Scheduler.async_scheduler = scheduler
            cls.executors = getattr(scheduler, "_executors", None)
            Scheduler.async_scheduler.add_listener(
                cls.listener_all_job,
                EVENT_JOB_ERROR | EVENT_JOB_MISSED | EVENT_JOB_MAX_INSTANCES |
                EVENT_ALL_JOBS_REMOVED | EVENT_JOB_ADDED | EVENT_JOB_REMOVED |
                EVENT_JOB_MODIFIED | EVENT_JOB_EXECUTED | EVENT_JOB_SUBMITTED
            )

    @staticmethod
    def configure(kind: SCHEDULER_KIND, **kwargs):
        if kind == "async":
            Scheduler.async_scheduler.configure(**kwargs)

    @classmethod
    def start(cls):
        Scheduler.async_scheduler.start()

    @staticmethod
    def add_job(func, tigger=None, _id: Optional[str] = None, job_store="default", executor="default", **kw):
        """增加任务会先查询是否存在任务，有的话会直接删除"""
        old_job = Scheduler.get_job(_id)
        if old_job is not None:
            old_job.remove()
        scheduler = Scheduler.async_scheduler
        return scheduler.add_job(
            func, tigger, id=_id,
            jobstore=job_store,
            executor=executor,
            **kw
        )

    @staticmethod
    def get_job(_id: str):
        return Scheduler.async_scheduler.get_job(_id)

    @staticmethod
    def is_job_exist(_id: str):
        return Scheduler.get_job(_id) is not None

    @classmethod
    def pause(cls, _id: str):
        job = Scheduler.get_job(_id)
        if job is not None:
            job.pause()

    @classmethod
    def resume(cls, _id: str):
        job = Scheduler.get_job(_id)
        if job is not None:
            job.resume()

    @classmethod
    def remove(cls, _id: str):
        job = Scheduler.get_job(_id)
        if job is not None:
            job.remove()

    @classmethod
    def register_event_handler(cls, task_kind_func, async_func):
        """
        针对定时任务 和 对象运行计划（尚未完成）中的一些监控、运行任务，运行过程中出现的状态变化
        需要记录到不同的数据库中。
        在event触发后进行处理，比如定时任务id以timedTask开头，那么需要使用func(id)得到true
        后触发后面的async_func函数，写入一些数据到对应的数据库
        :param task_kind_func:
        :param async_func:
        :return:
        """
        if not isinstance(task_kind_func, Callable) or not asyncio.iscoroutinefunction(async_func):
            raise TypeError
        cls.event_dispatch_dict[task_kind_func] = async_func

    @classmethod
    def listener_all_job(cls, event: JobEvent | JobSubmissionEvent | JobExecutionEvent):
        _event_loop = Scheduler.async_scheduler.__getattribute__("_eventloop")
        job_id = None
        if event.code != EVENT_ALL_JOBS_REMOVED:
            job_id = event.job_id
        if job_id is None:
            return
        trace_back = event.traceback if hasattr(event, 'traceback') else ""
        _exception = event.exception if hasattr(event, 'exception') else ""
        for func, event_async_func in cls.event_dispatch_dict.items():
            if func(job_id):
                _event_loop.create_task(event_async_func(
                    event.code, job_id, trace_back=trace_back, exc=_exception,
                ))
                break
