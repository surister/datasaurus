import contextlib
from io import BytesIO

with contextlib.suppress(ImportError):
    from azure.storage.blob import BlobServiceClient

from datasaurus.core.storage.base import Storage, AUTO_RESOLVE


class AzureBlobStorage(Storage):
    __slots__ = ('connect_str', 'container_name', 'encoding', 'container_client')

    def __init__(self,
                 connect_str: str,
                 container_name: str,
                 encoding: str = 'utf-8',
                 container_client=None,
                 name: str = '',
                 environment: str = AUTO_RESOLVE,
                 ):
        super().__init__(name, environment)
        self.connect_str = connect_str
        self.container_name = container_name
        self.encoding = encoding
        self.container_client = container_client

    @property
    def client(self):
        if self.container_client:
            return self.container_client

        blob_service_client = BlobServiceClient.from_connection_string(self.connect_str)
        return blob_service_client.get_container_client(container=self.container_name)

    def file_exists(self, file_name) -> bool:
        pass

    def write_file(self, file_name, data, create_table):
        pass

    def read_file(self, file_name: str):
        with BytesIO() as file:
            # We 'download' the data into a temporal BytesIO object.
            self.client.download_blob(file_name).readinto(file)
            file.seek(0)
            return file.read().decode(self.encoding)
