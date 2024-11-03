import traceback
from typing import cast

from fastapi import APIRouter, Query
from motor.core import AgnosticCollection

from server.timedTask.model import *
from server.timedTask.util import get_task_id, get_device_cpu_and_mem
from server.util import response_data_format
from utils.mongo_client import AsyncMongoClient
from utils.scheduler import Scheduler

router = APIRouter()

class ResponseCode:
    NO_PERMIT = -1
    SUCCESS = 0
    GENERAL_FAULT = 1


@router.post("/timedTask", summary="操作定时任务")
async def operate_timed_task(request: TimedTaskOperateModel):
    timed_task_collect = cast(AgnosticCollection, AsyncMongoClient["timed_task_collect"])
    response = {
        "code": ResponseCode.GENERAL_FAULT,
        "msg": None,
        "data": None
    }
    try:
        if request.operate == TimedTaskOperate.ADD:
            task_id = get_task_id(request.timedTaskKind)
            timed_task_data = TimedTaskModel(
                taskID=task_id,
                **request.model_dump(exclude={"taskID", }, by_alias=True)
            )
            new_timed_task = await timed_task_collect.insert_one(timed_task_data.model_dump(by_alias=True))
            timed_task_id = new_timed_task.inserted_id
            return_data = await timed_task_collect.find_one({
                "_id": timed_task_id, "isShow": True
            }, projection={'_id': False, "isShow": False})
            try:
                if request.timedTaskKind == TimedTaskKind.CPU_MEM_RECORD:
                    if request.interval is not None:
                        Scheduler.add_job(
                            get_device_cpu_and_mem,
                            "interval", _id=task_id,
                            seconds=request.interval,
                            start_date=request.planRunTime[0],
                            end_date=request.planRunTime[1],
                            args=(
                                timed_task_id, task_id, request.objIP,
                                request.loginUser, request.loginPassword,
                                request.sshUser, request.sshPassword
                            )
                        )
                    else:
                        ...
            except Exception as e:
                await timed_task_collect.find_one_and_update(
                    {"_id": timed_task_id}, {"$set": {"isShow": False}})
                raise Exception(e)
            response["code"] = ResponseCode.SUCCESS
            response["msg"] = f"{request.taskName}添加成功！"
            response['data'] = response_data_format(return_data)
    except Exception as e:
        print(traceback.format_exc())
        response['msg'] = f'操作定时任务数据失败：{e}'
    return response


@router.post("/timedTask/search", summary="查询现有的定时任务")
async def search_timed_task(request: GetTimedTaskModel):
    timed_task_collect = cast(AgnosticCollection, AsyncMongoClient["timed_task_collect"])
    response = {
        "code": ResponseCode.GENERAL_FAULT,
        "msg": None,
        "data": None
    }
    try:
        limit = request.limit
        page = request.page
        skip = limit * (page - 1)
        default_query_dict = {
            '$match': {
                'isShow': True
            }
        }
        sort_dict = {
            "$sort": {
                'updateTime': -1,
                # 'prodLine': -1,
                # 'prodSeries': -1
            }
        }
        first_project = {
            '$project': {
                'isShow': 0
            }
        }
        group_dict = {
            '$group': {
                '_id': None,
                'total': {
                    '$sum': 1
                },
                'list': {
                    '$push': '$$ROOT'
                }
            }
        }
        project_query_dict = {
            '$project': {
                '_id': 0,
                'total': 1,
                'list': {
                    '$slice': ['$list', skip, limit]
                }
            }
        }

        aggregate_conditions = [
            default_query_dict,
            sort_dict,
            first_project,
            group_dict,
            project_query_dict,
        ]
        if request.timed_task_id:
            default_query_dict["$match"]["_id"] = request.timed_task_id
        datas = await timed_task_collect.aggregate(aggregate_conditions).to_list(None)
        if len(datas) == 0:
            raise Exception("查询数据异常！")
        response["code"] = ResponseCode.SUCCESS
        response["msg"] = '获取定时任务数据成功'
        response["data"] = response_data_format(datas[0])
    except Exception as e:
        print(traceback.format_exc())
        response['msg'] = '获取定时任务数据失败:' + str(e)
    return response


@router.post('/timedTask/detail', summary="查看定时任务详情")
async def get_timed_task_detail(
        *,
        timed_task_id: PyandticObjectId = Query(..., alias="timedTaskID"),
        request: GetTimedTaskDetailModel
):
    timed_task_collect = cast(AgnosticCollection, AsyncMongoClient["timed_task_collect"])
    response = {
        "code": ResponseCode.GENERAL_FAULT,
        "msg": None,
        "data": None
    }
    try:
        time_interval_dict = []
        time_record_dict = []
        if request.startTime:
            time_record_dict.append({"$gte": ["$operateTime", request.startTime]})
            time_interval_dict.append({"$gte": ["$recordTime", request.startTime]})
        if request.endTime:
            time_record_dict.append({"$lte": ["$operateTime", request.endTime]})
            time_interval_dict.append({"$lte": ["$recordTime", request.endTime]})

        default_query_dict = {
            "$match": {
                "isShow": True,
                "_id": timed_task_id
            }
        }
        record_lookup_dict = [
            {
                '$lookup': {
                    'from': 'timed_task_record_collect',
                    'let': {
                        'timedTaskID': '$_id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {'$eq': ['$$timedTaskID', '$timedTaskID']},
                                        {'$eq': [True, '$isShow']},
                                        *time_record_dict
                                    ]
                                }
                            }
                        },
                        {'$sort': {'operateTime': -1}},
                        {
                            '$project': {
                                '_id': 0,
                                'isShow': 0,
                                'timedTaskID': 0
                            }
                        },
                        {
                            '$group': {
                                '_id': None,
                                'total': {
                                    '$sum': 1
                                },
                                'list': {
                                    '$push': '$$ROOT'
                                }
                            }
                        }
                    ],
                    'as': 'records'
                }
            },
            {
                '$unwind': {
                    'path': '$records',
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                '$project': {
                    'timedTaskKind': 1,
                    'recordsTotal': '$records.total',
                    'records': {
                        '$slice': [
                            '$records.list', request.record_limit * (request.record_page - 1), request.record_limit
                        ]
                    }
                }
            }
        ]
        result_lookup_dict = [
            {
                '$lookup': {
                    'from': "",
                    'let': {
                        'timedTaskID': '$_id'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {'$eq': ['$$timedTaskID', '$timedTaskID']},
                                        {'$eq': [True, '$isShow']},
                                        *time_interval_dict
                                    ]
                                }
                            }
                        },
                        {'$sort': {'recordTime': -1}},
                        {
                            '$project': {
                                '_id': 0,
                                'isShow': 0,
                                'timedTaskID': 0
                            }
                        },
                        {
                            '$group': {
                                '_id': None,
                                'total': {
                                    '$sum': 1
                                },
                                'list': {
                                    '$push': '$$ROOT'
                                }
                            }
                        }
                    ],
                    'as': 'results'
                }
            },
            {
                '$unwind': {
                    'path': '$results',
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                '$project': {
                    'timedTaskKind': 1,
                    "records": 1,
                    "recordsTotal": 1,
                    'resultsTotal': '$results.total',
                    'results': {
                        '$slice': [
                            '$results.list', (request.result_page - 1) * request.result_limit, request.result_limit
                        ]
                    }
                }
            }
        ]
        project_dict = {
            "$project": {
                "_id": 0,
                "isShow": 0
            }
        }

        task_info = await timed_task_collect.find_one(
            {"_id": timed_task_id, "isShow": True}
        )
        if task_info is None:
            raise Exception("未找到该定时任务")
        result_collect_name: str
        if task_info["timedTaskKind"] == TimedTaskKind.CPU_MEM_RECORD:
            result_lookup_dict[0]["$lookup"]["from"] = "timed_task_dev_cpu_mem_collect"
            result_lookup_dict[0]["$lookup"]["pipeline"].pop()
            result_lookup_dict.pop()
            result_lookup_dict.pop()
            result_lookup_dict[0]["$lookup"]["pipeline"][1]["$sort"]["recordTime"] = 1
        else:
            raise Exception("暂时只有设备运行状况类型定时任务结果")

        aggregates_conditions = [
            default_query_dict,
            *record_lookup_dict,
            *result_lookup_dict,
            project_dict
        ]
        print(aggregates_conditions)
        datas = await timed_task_collect.aggregate(aggregates_conditions).to_list(None)
        if len(datas) == 0:
            raise Exception('获取数据异常，请联系113')
        response["code"] = ResponseCode.SUCCESS
        response["msg"] = '获取定时任务数据成功'
        response["data"] = response_data_format(datas[0])
    except Exception as e:
        print(traceback.format_exc())
        response['msg'] = '定时任务数据查询失败:' + str(e)
    return response
