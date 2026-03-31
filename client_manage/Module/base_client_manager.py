import io
import os

from abc import ABC, abstractmethod

from client_manage.Method.path import createFileFolder, removeFile


class BaseClientManager(ABC):
    def __init__(self) -> None:
        self.common_client = self.createClient()
        return

    @abstractmethod
    def createClient(self):
        pass

    @abstractmethod
    def getObjectStreamWithClient(
        self,
        client,
        data_url_path: str,
    ) -> io.BytesIO:
        pass

    def getObjectStream(
        self,
        data_url_path: str,
        use_common_client: bool=False,
    ) -> io.BytesIO:
        if use_common_client:
            return self.getObjectStreamWithClient(
                client=self.common_client,
                data_url_path=data_url_path,
            )

        return self.getObjectStreamWithClient(
            client=self.createClient(),
            data_url_path=data_url_path
        )

    @abstractmethod
    def downloadObjectWithClient(
        self,
        client,
        data_url_path: str,
        data_file_path: str,
    ) -> bool:
        pass

    def downloadObject(
        self,
        data_url_path: str,
        data_file_path: str,
        use_common_client: bool=False,
        overwrite: bool=False,
        print_progress: bool=True,
    ) -> bool:
        if os.path.exists(data_file_path):
            if not overwrite:
                return True

            removeFile(data_file_path)

        createFileFolder(data_file_path)

        if print_progress:
            print('[INFO][BaseClientManager::downloadObject]')
            print('\t start download object:')
            print('\t', data_url_path)
            print('\t -->')
            print('\t', data_file_path)

        if use_common_client:
            if not self.downloadObjectWithClient(
                client=self.common_client,
                data_url_path=data_url_path,
                data_file_path=data_file_path,
            ):
                print('[ERROR][BaseClientManager::downloadObject]')
                print('\t downloadObjectWithClient failed!')
                return False

        if not self.downloadObjectWithClient(
            client=self.createClient(),
            data_url_path=data_url_path,
            data_file_path=data_file_path,
        ):
            print('[ERROR][BaseClientManager::downloadObject]')
            print('\t downloadObjectWithClient failed!')
            return False

        if print_progress:
            print('[INFO][BaseClientManager::downloadObject]')
            print('\t download object finished!')
        return True

    @abstractmethod
    def uploadObjectWithClient(
        self,
        client,
        data_file_path: str,
        data_url_path: str,
    ) -> bool:
        pass

    def uploadObject(
        self,
        data_file_path: str,
        data_url_path: str,
        use_common_client: bool=False,
        remove_after_upload: bool=True,
        print_progress: bool=True,
    ) -> bool:
        if not os.path.exists(data_file_path):
            print('[ERROR][BaseClientManager::uploadObject]')
            print('\t data file not exist!')
            print('\t data_file_path:', data_file_path)
            return False

        if print_progress:
            print('[INFO][BaseClientManager::uploadObject]')
            print('\t start upload object:')
            print('\t', data_file_path)
            print('\t -->')
            print('\t', data_url_path)

        if use_common_client:
            if not self.uploadObjectWithClient(
                client=self.common_client,
                data_file_path=data_file_path,
                data_url_path=data_url_path,
            ):
                print('[ERROR][BaseClientManager::uploadObject]')
                print('\t uploadObjectWithClient failed!')
                return False

        if not self.uploadObjectWithClient(
            client=self.createClient(),
            data_file_path=data_file_path,
            data_url_path=data_url_path,
        ):
            print('[ERROR][BaseClientManager::uploadObject]')
            print('\t uploadObjectWithClient failed!')
            return False

        if print_progress:
            print('[INFO][BaseClientManager::uploadObject]')
            print('\t upload object finished!')

        if remove_after_upload:
            removeFile(data_file_path)
        return True
