import os
import traceback
import uuid

from io import BytesIO

import aiohttp
from aiohttp import ClientResponse
from miniopy_async import Minio as AsyncMinio

from src.config import Config


def object_filename_and_ext(object_name) -> (str, str):
    return os.path.splitext(object_name)


class _SingleInstance(type):

    def __init__(cls, *args, **kwargs):
        cls._instance = None
        super().__init__(*args, **kwargs)

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class AsyncMinioClient(metaclass=_SingleInstance):
    minio_client = None

    def __init__(
            self,
            endpoint: str,
            access_key: str,
            secret_key: str,
            secure: bool = False):
        self.endpoint = endpoint
        try:
            self.minio_client = AsyncMinio(
                self.endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
        except Exception as e:
            raise Exception("粗糙的表示minio async_client初始化失败！")

    async def file_put_object(self, bucket_name, object_name, file_path,
                              content_type='application/octet-stream', auto_id=False):
        """
        Description: 以文件的方式上传
        :param bucket_name: Bucket to read object from.
        :param object_name: Name of the object to read.
        :param file_path: Local file path to be uploaded.
        :param content_type: Content type of the object.
        :param auto_id: True or False，为True时表示会自动加上唯一标识的UUID
        :return: None or url
        """
        if auto_id:
            file_name, file_extension = object_filename_and_ext(object_name)
            object_name = file_name + '.' + str(uuid.uuid1()) + file_extension
        await self.minio_client.fput_object(
            bucket_name,  # bucket名称
            object_name,  # object_name
            file_path,  # 要上传的文件
            content_type
        )
        return self.make_download_url(bucket_name, object_name)

    def make_download_url(self, bucket_name: str, object_name: str):
        """
        通过url get直接下载文件，但是需要将minio对应bucket设置成public？？
        :param bucket_name:
        :param object_name:
        :return:
        """
        return 'http://{endpoint}/{bucket_name}/{object_name}'.format(
            endpoint=self.endpoint, bucket_name=bucket_name, object_name=object_name)

    async def put_object(self, bucket_name, object_name, data, length, auto_id=True):
        """
        Description: 上传对象
        :param bucket_name: 新对象存储的bucket name.
        :param object_name: 新对象名称.
        :param data:  上传数据.
        :param length: 对象总长度.
        :param auto_id: True or False，为True时表示会自动加上唯一标识的UUID
        :return: None or url
        """
        try:
            if auto_id:
                file_name, file_extension = object_filename_and_ext(object_name)
                object_name = file_name + '.' + str(uuid.uuid1()) + file_extension
            await self.minio_client.put_object(
                bucket_name,  # bucket名称
                object_name,  # object_name
                data,  # 要上传的文件
                length)
            return self.make_download_url(bucket_name, object_name)
        except Exception as err:
            print(err)
        return None

    async def put_object_by_buffer(self, bucket_name, file_data, sub_path='', object_name=None):
        """
        Description: 上传对象，适用于文件流，类似于HTTP网络流
        :param bucket_name: bucket名称
        :param file_data: 文件流,类似于HTTP网络流
        :param sub_path: 存储路径
        :param object_name: 对象名称
        :return: None or url
        """
        try:
            object_name = sub_path + (str(file_data) if not object_name else object_name)
            data = file_data
            length = file_data.size
            return await self.put_object(bucket_name, object_name, data, length)
        except Exception as err:
            print(err)
        return None

    async def cut_object(self, bucket_name, object_name, object_source, auto_id=True):
        """
        Description: 用于操作将Minio上的一个资源移到另外一个目录中
        :param bucket_name: 新对象的bucket名称
        :param object_name: 新对象的名称
        :param object_source: 新对象的源  "/my-sourcebucketname/my-sourceobjectname"
        :param auto_id: True or False，为True时表示会自动加上唯一标识的UUID
        :return: None or url
        """
        try:
            if auto_id:
                file_name, file_extension = object_filename_and_ext(object_name)
                object_name = file_name + '.' + str(uuid.uuid1()) + file_extension
            # 拷贝 bucket
            await self.minio_client.copy_object(bucket_name, object_name, object_source)
            await self.minio_client.remove_object(bucket_name, object_source)
            return self.make_download_url(bucket_name, object_name)
        except Exception as err:
            print(err)
            return False

    async def get_object_from_dir(self, bucket_name: str, obj_name: str):
        res = []
        list_bucket_objs = await self.minio_client.list_objects(bucket_name, prefix=obj_name)
        for obj in list_bucket_objs:
            if obj.object_name == obj_name:
                if obj_name.endswith("/"):
                    continue
                else:
                    res.append(obj.object_name)
                    continue
            if obj.object_name.endswith("/"):
                res += await self.get_object_from_dir(bucket_name, obj.object_name)
            else:
                res.append(obj.object_name)
        return res

    async def remove_object(self, bucket_name: str, obj_name: str):
        list_bucket_objs = await self.minio_client.list_objects(bucket_name, obj_name, recursive=True)
        for obj in list_bucket_objs:
            await self.minio_client.remove_object(bucket_name, obj.object_name)

    async def download_file(self, bucket_name: str, object_name: str, file_path: str, mk_dir=True, split_fist=False):
        list_bucket_objs = await self.get_object_from_dir(bucket_name, object_name)
        for item in list_bucket_objs:
            if mk_dir is True:
                if split_fist is True and "/" in item:
                    _item = "/".join(item.split("/")[1:])
                else:
                    _item = item
                _file_path = os.path.join(file_path, os.path.dirname(_item))
                if not os.path.exists(_file_path):
                    os.makedirs(_file_path)
            else:
                _file_path = file_path
            minio_file_name = os.path.basename(item).replace(":", "_")
            await self.minio_client.fget_object(bucket_name, item, os.path.join(_file_path, minio_file_name))
        return None

    async def presigned_get_object(self, bucket_name, object_name):
        # Request URL expired after 7 days
        url = await self.minio_client.presigned_get_object(
            bucket_name=bucket_name,
            object_name=object_name
        )
        return url

    async def list_buckets(self):
        return await self.minio_client.list_buckets()

    async def list_objects(self):
        res = []
        list_bucket = await self.minio_client.list_buckets()
        for bucket in list_bucket:
            res += await self.minio_client.list_objects(bucket.name, recursive=True)
        return res

    async def make_bucket_sub_path(self, bucket_name: str, sub_path: str):
        """
        无文件上传的情况下，在bucket下创建子目录
        :param bucket_name:
        :param sub_path:
        :return:
        """
        try:
            if not sub_path.endswith("/"):
                sub_path += "/"
            data = BytesIO()
            await self.minio_client.put_object(
                bucket_name,  # bucket名称
                sub_path,  # object_name
                data,  # 要上传的文件
                0)
            return True
        except Exception:
            print(traceback.format_exc())
            return False

    async def sub_path_exists(self, bucket_name: str, sub_path: str):
        """
        判断子目录是否存在
        :param bucket_name:
        :param sub_path:
        :return:
        """
        return list(await self.minio_client.list_objects(bucket_name, sub_path)) != []

    async def async_iter_get_object(self, bucket_name: str, object_name: str):
        offset, length = 0, 1024 * 10
        session = aiohttp.ClientSession()
        list_bucket_objs = await self.get_object_from_dir(bucket_name, object_name)
        try:
            for item in list_bucket_objs:
                while True:
                    rsp: ClientResponse = await self.minio_client.get_object(
                        bucket_name, item, session, offset=offset, length=length,
                    )
                    content = await rsp.content.read()
                    yield content
                    if len(content) < length:
                        break
                    offset += length
                offset, length = 0, 1024 * 10
        except Exception as err:
            print(err)
        finally:
            await session.close()


async_minio_client = AsyncMinioClient(
    endpoint=Config.minio_endpoint,
    access_key=Config.minio_access_key,
    secret_key=Config.minio_secret_key
)


