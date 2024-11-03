import json
import itertools
from datetime import datetime
from bson import ObjectId


num_counter = itertools.count(1).__next__


def get_current_time_and_num():
    """
    给定时任务制作的序号：时间+序号
    :return:
    """
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{current_time}{(num_counter() % 999999 + 1):06}"


class ResponseDataEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime):
            # 得注意时区
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return super().default(obj)


def response_data_format(data):
    try:
        return json.loads(json.dumps(data, cls=ResponseDataEncoder))
    except Exception:
        return data
