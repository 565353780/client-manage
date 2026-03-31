import io

from typing import Tuple

from client_manage.Client.cos import get_cos_client
from client_manage.Module.base_client_manager import BaseClientManager


class COSClientManager(BaseClientManager):
    def __init__(self) -> None:
        super().__init__()
        return

    def splitBucketAndKey(
        self,
        data_url_path: str,
    ) -> Tuple[str, str]:
        bucket = data_url_path.split('/')[0]
        key = data_url_path[len(bucket) + 1:]
        return bucket, key

    def createClient(self):
        return get_cos_client()

    def getObjectStreamWithClient(
        self,
        client,
        data_url_path: str,
    ) -> io.BytesIO:
        bucket, key = self.splitBucketAndKey(data_url_path)

        response = client.get_object(Bucket=bucket, Key=key)
        obj_stream = io.BytesIO(response['Body'].get_raw_stream().read())
        return obj_stream

    def downloadObjectWithClient(
        self,
        client,
        data_url_path: str,
        data_file_path: str,
    ) -> bool:
        bucket, key = self.splitBucketAndKey(data_url_path)

        client.download_file(
            Bucket=bucket, # Bucket名称
            Key=key,      # 上传到COS后的对象键
            DestFilePath=data_file_path, # 本地文件路径
            PartSize=20,                    # 分块大小(MB)，默认可不设
            MAXThread=10,                   # 并发线程数
        )
        return True

    def uploadObjectWithClient(
        self,
        client,
        data_file_path: str,
        data_url_path: str,
    ) -> bool:
        bucket, key = self.splitBucketAndKey(data_url_path)

        response = client.upload_file(
            Bucket=bucket, # Bucket名称
            Key=key,      # 上传到COS后的对象键
            LocalFilePath=data_file_path, # 本地文件路径
            PartSize=20,                    # 分块大小(MB)，默认可不设
            MAXThread=10,                   # 并发线程数
            EnableMD5=False,                # 开启MD5校验，大文件建议关闭以提速
        )

        '''
        if 'ETag' not in response:
            return False
        '''

        return True
