import io
import os

from typing import Tuple

from client_manage.Client.cos import get_cos_client
from client_manage.Method.path import removeFile, createFileFolder
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

    def getObjectStreamDataWithClient(
        self,
        client,
        data_url_path: str,
    ) -> io.BytesIO:
        bucket, key = self.splitBucketAndKey(data_url_path)

        response = client.get_object(Bucket=bucket, Key=key)
        stream_data = io.BytesIO(response['Body'].get_raw_stream().read())
        return stream_data

    def downloadObjectWithClient(
        self,
        client,
        data_url_path: str,
        data_file_path: str,
        overwrite: bool=False,
        print_progress: bool=True,
    ) -> bool:
        if os.path.exists(data_file_path):
            if not overwrite:
                return True

            removeFile(data_file_path)

        bucket, key = self.splitBucketAndKey(data_url_path)

        createFileFolder(data_file_path)

        if print_progress:
            print('[INFO][COSClientManager::downloadObjectWithClient]')
            print('\t start download object:')
            print(f'\t {bucket}/{key}')
            print('\t -->')
            print('\t', data_file_path)

        client.download_file(
            Bucket=bucket, # Bucket名称
            Key=key,      # 上传到COS后的对象键
            DestFilePath=data_file_path, # 本地文件路径
            PartSize=20,                    # 分块大小(MB)，默认可不设
            MAXThread=10,                   # 并发线程数
        )

        if print_progress:
            print('[INFO][COSClientManager::downloadObjectWithClient]')
            print('\t download_file finished!')

        return True

    def uploadObjectWithClient(
        self,
        client,
        data_file_path: str,
        data_url_path: str,
        remove_after_upload: bool=True,
        print_progress: bool=True,
    ) -> bool:
        if not os.path.exists(data_file_path):
            print('[ERROR][COSClientManager::uploadObjectWithClient]')
            print('\t data file not exist!')
            print('\t data_file_path:', data_file_path)
            return False

        bucket, key = self.splitBucketAndKey(data_url_path)

        if print_progress:
            print('[INFO][COSClientManager::uploadObjectWithClient]')
            print('\t start upload object:')
            print('\t', data_file_path)
            print('\t -->')
            print(f'\t {bucket}/{key}')

        response = client.upload_file(
            Bucket=bucket, # Bucket名称
            Key=key,      # 上传到COS后的对象键
            LocalFilePath=data_file_path, # 本地文件路径
            PartSize=20,                    # 分块大小(MB)，默认可不设
            MAXThread=10,                   # 并发线程数
            EnableMD5=False,                # 开启MD5校验，大文件建议关闭以提速
        )

        if print_progress:
            print('[INFO][COSClientManager::uploadObjectWithClient]')
            print('\t upload_file finished!')
            print('\t response:', response['ETag']) # 成功后返回文件的ETag

        if remove_after_upload:
            removeFile(data_file_path)

        return True
