from urllib.parse import quote_plus

class Config:
    MONGO_STR = f'mongodb://{quote_plus("MyTestWeb")}:{quote_plus("mytestweb")}@127.0.0.1:27017/MyTestWeb'
    MONGO_DATABASE = 'MyTestWeb'

    minio_endpoint = '127.0.0.1:10008'
    minio_access_key = 'minioadmin'
    minio_secret_key = 'mianshimianshi'
