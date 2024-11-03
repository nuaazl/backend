import time
import itertools
import traceback

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_JOB_EXECUTED, EVENT_JOB_REMOVED
from motor.core import AgnosticCollection
from pymongo import ReturnDocument

from server.timedTask.model import (
    TimedTaskKind,
    TaskStatus,
    TimedTaskSysRecordModel,
    PyandticObjectId,
    TimedTaskDevCPUAndMEMModel
)
from utils.pydis import Pydis
from utils.mongo_client import AsyncMongoClient
from utils.scheduler import Scheduler, DistributedLockAcquireError, job_lock

_task_num_counter = itertools.count(1).__next__


def get_task_id(kind: TimedTaskKind, pre="TimedTask"):
    return f"{pre}_{int(time.time())}_{kind}_{_task_num_counter()}"


@job_lock(expire_after_seconds=15)
async def get_device_cpu_and_mem(
        timed_task_id: PyandticObjectId,
        task_id: str,
        ip: str,
        ssh_username: str,
        ssh_password: str
):
    # 由于没有例子把这块注释掉
    # client = await Pydis.get_ssh_client(ip, ssh_username, ssh_password)
    # recv, flag = await client.send_and_recv("vmstat | awk 'NR==3 {print $3,$4,$5,$6,$9,$10,$13,$14,$15,$16,$17}'")
    # if flag is False:
    #     raise Exception(f"返回的信息为：{recv}")
    # swpd_mem, free_mem, buff_mem, cache_mem, bi_io, bo_io, us_cpu, sy_cpu, id_cpu, wa_cpu, st_cpu = \
    #     recv.split()

    # 示例
    swpd_mem, free_mem, buff_mem, cache_mem, bi_io, bo_io, us_cpu, sy_cpu, id_cpu, wa_cpu, st_cpu = \
        "5452595", "3352595", "2152595", "52595", "10", "10", "70", "10", "10", "1", "2"
    timed_task_dev_cpu_mem_collect: AgnosticCollection = AsyncMongoClient["timed_task_dev_cpu_mem_collect"]
    new_data = await timed_task_dev_cpu_mem_collect.insert_one(
        TimedTaskDevCPUAndMEMModel(
            taskID=task_id,
            timedTaskID=timed_task_id,
            swpdMem=float(swpd_mem),
            freeMem=float(free_mem),
            buffMem=float(buff_mem),
            cacheMem=float(buff_mem),
            biIo=float(bi_io),
            boIo=float(bo_io),
            usCpu=float(us_cpu) / 100,
            syCpu=float(sy_cpu) / 100,
            idCpu=float(id_cpu) / 100,
            waCpu=float(wa_cpu) / 100,
            stCpu=float(st_cpu) / 100
        ).model_dump()
    )
    print(new_data.inserted_id)


async def handle_event_timed_task(event_code: int, job_id: str, **kwargs):
    try:
        if job_id is None:
            return
        update_data = None
        result = None
        # trace_back = kwargs.get("trace_back")
        exc = kwargs.get("exc")
        if event_code == EVENT_JOB_ERROR:
            if isinstance(exc, DistributedLockAcquireError):
                return
            update_data = {
                "$set": {
                    "taskStatus": TaskStatus.ERROR
                }
            }
            result = f"执行出错了！{exc}"
        if event_code == EVENT_JOB_MISSED:
            update_data = {
                "$set": {
                    "taskStatus": TaskStatus.MISSED
                }
            }
            result = f"定时任务错过了执行时间！"
        if event_code == EVENT_JOB_EXECUTED:
            update_data = {
                "$inc": {
                    "taskRunCounts": 1
                }
            }
            result = "定时任务执行！"
        if event_code == EVENT_JOB_REMOVED:
            update_data = {
                "$set": {
                    "taskStatus": TaskStatus.DELETED,
                    "isShow": False
                }
            }
        job = Scheduler.get_job(job_id)
        if job is None or job.next_run_time is None:
            update_data = {
                "$set": {
                    "taskStatus": TaskStatus.COMPLETED
                }
            }
        if update_data is not None:
            print(update_data)
            timed_task_collect: AgnosticCollection = AsyncMongoClient["timed_task_collect"]
            return_data = await timed_task_collect.find_one_and_update(
                {"taskID": job_id},
                update_data,
                return_document=ReturnDocument.AFTER
            )
            print(return_data)
            if result is not None:
                timed_task_record = TimedTaskSysRecordModel(
                    timedTaskID=return_data["_id"],
                    taskName=return_data["taskName"],
                    taskID=job_id,
                    operateResult=result
                )
                timed_task_record_collect: AgnosticCollection = AsyncMongoClient["timed_task_record_collect"]
                res = await timed_task_record_collect.insert_one(timed_task_record.model_dump())
                print(res.inserted_id)
    except Exception as e:
        print(traceback.format_exc())


def is_timed_task(task_id: str):
    return task_id.startswith("TimedTask")


Scheduler.register_event_handler(is_timed_task, handle_event_timed_task)
