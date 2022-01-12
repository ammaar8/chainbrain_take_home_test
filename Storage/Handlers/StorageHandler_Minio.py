from minio import Minio
from minio.credentials.providers import ClientGrantsProvider
from minio.error import S3Error

from Storage.Handlers.StorageHandler import StorageHandler


class StorageHandler_Minio(StorageHandler):
    """Handles reading and writing datasets to Minio for StorageSession."""

    def __init__(self) -> None:
        super().__init__(
            required_keys=[
                "endpoint",
                "access_key",
                "secret_key",
            ],
        )

    def check_connection(self, connection_keys) -> bool:
        """Test connection with given keys to make sure connection can be made."""
        try:
            client = Minio(**connection_keys)
            client.list_buckets()
        except S3Error:
            return False
        except:
            return False
        return True

    def getDataset(self, filename, connection_keys, bucket):
        """Gets a dataset from storage.

        Args:
            filename: A string by which the stored object is to be identified by.
            connection_keys: Dictionary of keys with values required to connect
                to the storage.
            bucket: Bucket name.

        Returns:
            An arrow table object.

        """
        client = Minio(**connection_keys)
        response = client.get_object(bucket, filename)
        dataset = self.parquetToObject(response.data)
        return dataset

    def writeDataset(self, filename, data_object, connection_keys, bucket):
        """Writes a given object to storage.

        Args:
            filename: A string by which the stored object is to be identified by.
            data_object: An arrow table object.
            connection_keys: Dictionary of keys with values required to connect
                to the storage.
            bucket: Bucket name.

        Returns:
            Whether writing to storage succeded as True or False.

        """
        try:
            parquetObj = self.objectToParquet(data_object)
            parquetObj.seek(0)
            client = Minio(**connection_keys)
            length = parquetObj.getbuffer().nbytes
            client.put_object(bucket, filename, parquetObj, length=length)
            return True
        except Exception as e:
            return False
