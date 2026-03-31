import io

from abc import ABC, abstractmethod


class BaseClientManager(ABC):
    def __init__(self) -> None:
        self.common_client = self.createClient()
        return

    @abstractmethod
    def createClient(self):
        pass

    @abstractmethod
    def getObjectStreamDataWithClient(
        self,
        client,
        data_url_path: str,
    ) -> io.BytesIO:
        pass

    def getObjectStreamData(
        self,
        data_url_path: str,
        use_common_client: bool=False,
    ) -> io.BytesIO:
        if use_common_client:
            return self.getObjectStreamDataWithClient(
                client=self.common_client,
                data_url_path=data_url_path,
            )

        return self.getObjectStreamDataWithClient(
            client=self.createClient(),
            data_url_path=data_url_path
        )

    @abstractmethod
    def downloadObjectWithClient(
        self,
        client,
        data_url_path: str,
        data_file_path: str,
        overwrite: bool=False,
        print_progress: bool=True,
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
        if use_common_client:
            return self.downloadObjectWithClient(
                client=self.common_client,
                data_url_path=data_url_path,
                data_file_path=data_file_path,
                overwrite=overwrite,
                print_progress=print_progress,
            )

        return self.downloadObjectWithClient(
            client=self.createClient(),
            data_url_path=data_url_path,
            data_file_path=data_file_path,
            overwrite=overwrite,
            print_progress=print_progress,
        )

    @abstractmethod
    def uploadObjectWithClient(
        self,
        client,
        data_file_path: str,
        data_url_path: str,
        remove_after_upload: bool=True,
        print_progress: bool=True,
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
        if use_common_client:
            return self.uploadObjectWithClient(
                client=self.common_client,
                data_file_path=data_file_path,
                data_url_path=data_url_path,
                remove_after_upload=remove_after_upload,
                print_progress=print_progress,
            )

        return self.uploadObjectWithClient(
            client=self.createClient(),
            data_file_path=data_file_path,
            data_url_path=data_url_path,
            remove_after_upload=remove_after_upload,
            print_progress=print_progress,
        )
