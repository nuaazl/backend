from datetime import datetime
from enum import auto, IntEnum
from typing import Optional, List, Tuple

from pydantic import field_validator, BaseModel, Field

from server.util import get_current_time_and_num
from server.model import PyandticObjectId


class TimedTaskKind(IntEnum):
    CPU_MEM_RECORD = 0


class TimedTaskOperate(IntEnum):
    ADD = 0
    EDIT = 1
    STOP = 2
    PAUSE = 3
    DELETE = 4


class TaskStatus(IntEnum):
    """0:待执行 1:执行完成 2:执行异常 3:已删除 4:执行错过"""
    PENDING = 0
    COMPLETED = 1
    ERROR = 2
    DELETED = 3
    MISSED = 4


class TimedTaskModel(BaseModel):
    task_id: Optional[str] = Field(description="任务id", default=None, alias="taskID")
    task_no: Optional[str] = Field(description="任务编号", default_factory=get_current_time_and_num, alias="taskNo")
    task_name: str = Field(description="任务名称", alias="taskName")
    timedTaskKind: TimedTaskKind = Field(description="任务类型")

    task_status: TaskStatus = Field(description="任务状态", default=TaskStatus.PENDING, alias="taskStatus")
    task_run_counts: int = Field(description="执行次数", default=0, alias="taskRunCounts")

    create_user: str = Field(description="创建人", default="应聘员2", alias="createUser")
    create_time: datetime = Field(description="创建时间", default_factory=datetime.now, alias="createTime")
    update_user: str = Field(description="更新人", default="应聘员2", alias="updateUser")
    update_time: datetime = Field(description="更新时间", default_factory=datetime.now, alias="updateTime")

    resource_id: Optional[PyandticObjectId] = Field(description="资源ID", default=None, alias="resourceID")

    obj_ip: Optional[str] = Field(description="对象ip", default=None, alias="objIP")
    obj_ssh_user: Optional[str] = Field(description="ssh用户名", default=None, alias="objSshUser")
    obj_ssh_password: Optional[str] = Field(description="ssh密码", default=None, alias="objSshPassword")

    crontab: Optional[str] = Field(description="crontab表达式", default=None)
    interval: Optional[int] = Field(description="执行时间间隔", default=None)
    plan_execute_time: Optional[Tuple[datetime, datetime]] = Field(
        description="计划执行时间区间", default=None, alias="planExecuteTime")

    is_show: bool = Field(description="是否存在", default=True, alias="isShow")

    @field_validator("create_time", "update_time")
    def check(cls, value: datetime):
        try:
            return value.astimezone().replace(tzinfo=None)
        except Exception as _:
            return value

    @field_validator("plan_execute_time")
    def double_time_check(cls, values):
        try:
            if len(values) != 2:
                raise Exception
            return list(map(lambda x: x.astimezone().replace(tzinfo=None), values))
        except Exception as _:
            return values


class TimedTaskSysRecordModel(BaseModel):
    """
    记录定时任务的执行情况
    """
    task_id: str = Field(description="任务id", alias="taskID")
    task_name: str = Field(description="任务名称", alias="taskName")
    operate_time: datetime = Field(description="执行时间", default_factory=datetime.now, alias="operateTime")
    operate_result: Optional[str] = Field(description="执行结果", alias="operateResult")
    timed_task_id: Optional[PyandticObjectId] = Field(description="关联的定时任务id", alias="timedTaskID")
    is_show: bool = Field(description="是否存在", default=True, alias="isShow")

    @classmethod
    @field_validator("operate_time")
    def check(cls, value: datetime):
        try:
            return value.astimezone().replace(tzinfo=None)
        except Exception as _:
            return value


class TimedTaskDevCPUAndMEMModel(BaseModel):
    task_id: str = Field(description="任务id", alias="taskID")
    record_time: Optional[datetime] = Field(description="记录时间", default_factory=datetime.now, alias="recordTime")
    free_mem: Optional[float] = Field(description="设备空闲内存，KB", alias="freeMem")
    swpd_mem: Optional[float] = Field(
        description="使用的交换内存量，当swpd值大于0时，说明服务器的物理内存不足，"
                    "需要查看程序是否存在内存泄漏，如果确定不是的话需要增加服务器的物理内存，KB",
        alias="swpdMem")
    buff_mem: Optional[float] = Field(description="用作缓冲区的内存量，KB", alias="buffMem")
    cache_mem: Optional[float] = Field(description="活动内存的数量", alias="cacheMem")
    bi_io: Optional[float] = Field(description=" 从块设备接收到的Kib byte（KiB/s）", alias="biIo")
    bo_io: Optional[float] = Field(description="发送到块设备的Kib byte(KiB/s)", alias="boIo")
    us_cpu: Optional[float] = Field(description="运行非内核代码所使用的cpu", alias="usCpu")
    sy_cpu: Optional[float] = Field(description="运行内核代码所使用的cpu", alias="syCpu")
    wa_cpu: Optional[float] = Field(
        description="等待IO所使用的cpu，如果wa值过高，则说明io等待比较严重，这可能是由于磁盘大量随机访问造成的，"
                    "可有可能是磁盘的带宽出现了瓶颈",
        alias="waCpu")
    st_cpu: Optional[float] = Field(description="从虚拟机窃取所使用的cpu", alias="stCpu")
    id_cpu: Optional[float] = Field(description="设备空闲CPU，小数", alias="idCpu")
    timed_task_id: Optional[PyandticObjectId] = Field(description="关联的定时任务id", alias="timedTaskID")
    is_show: Optional[bool] = Field(description="是否存在", default=True, alias="isShow")

    @field_validator("record_time")
    def check(cls, value: datetime):
        try:
            return value.astimezone().replace(tzinfo=None)
        except Exception as _:
            return value


class TimedTaskOperateModel(TimedTaskModel):
    operate: TimedTaskOperate = Field(description="操作类型")


class GetTimedTaskModel(BaseModel):
    # role: Role = Field(description='角色信息')
    page: int = Field(description="要查询的页数")
    limit: int = Field(description="单页显示的条数")
    timed_task_id: Optional[PyandticObjectId] = Field(description="要查询的定时任务ID", default=None, alias="timedTaskID")


class GetTimedTaskDetailModel(BaseModel):
    user_name: Optional[str] = Field(description='查询的用户信息', default="应聘人2", alias="userName")
    start_time: Optional[datetime] = Field(description='开始时间', default=None, alias="startTime")
    end_time: Optional[datetime] = Field(description='结束时间', default=None, alias="endTime")
    record_page: int = Field(description="要查询的页数", ge=1, alias="recordPage")
    record_limit: int = Field(description="单页显示的条数", ge=1, alias="recordLimit")
    result_page: int = Field(description="result要查询的页数", ge=1, alias="resultPage")
    result_limit: int = Field(description="result单页显示的条数", ge=1, alias="resultLimit")

    @field_validator("start_time", "end_time")
    def check(cls, value: datetime):
        try:
            return value.astimezone().replace(tzinfo=None)
        except Exception as _:
            return value
