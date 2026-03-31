import io

from client_manage.Module.base_client_manager import BaseClientManager


class MFSClientManager(BaseClientManager):
    def __init__(self) -> None:
        super().__init__()
        return

    def createClient(self):
        pass

    def getObjectStreamDataWithClient(
        self,
        client,
        data_url_path: str,
    ) -> io.BytesIO:
        pass
