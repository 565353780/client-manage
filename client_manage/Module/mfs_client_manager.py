try:
    import torch
except:
    pass

import MFSClient

import io
from typing import Tuple

from client_manage.Module.base_client_manager import BaseClientManager


class MFSClientManager(BaseClientManager):
    def __init__(self) -> None:
        super().__init__()
        return

    def splitTaskAndKey(
        self,
        data_url_path: str,
    ) -> Tuple[str, str]:
        task = data_url_path.split('/')[0]
        key = data_url_path[len(task) + 1:]
        return task, key

    def createClient(self):
        return MFSClient.MFSClient2()

    def getObjectStreamWithClient(
        self,
        client,
        data_url_path: str,
    ) -> io.BytesIO:
        task, key = self.splitTaskAndKey(data_url_path)

        obj_bytes = client.request_file(task, key)
        obj_stream = io.BytesIO(obj_bytes)
        return obj_stream

    def downloadObjectWithClient(
        self,
        client,
        data_url_path: str,
        data_file_path: str,
    ) -> bool:
        obj_stream = self.getObjectStreamWithClient(
            client=client,
            data_url_path=data_url_path,
        )

        with open(data_file_path, 'wb') as f:
            obj_stream.seek(0)  # Ensure pointer is at the start
            f.write(obj_stream.read())

        return True

    def uploadObjectWithClient(
        self,
        client,
        data_file_path: str,
        data_url_path: str,
    ) -> bool:
        print("[ERROR][MFSClientManager::uploadObjectWithClient]")
        print('\t this func has not finished!')
        return False
