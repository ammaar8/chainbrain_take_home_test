from Storage.Handlers.StorageHandler import StorageHandler


class StorageHandler_AWS(StorageHandler):
    """
    Handles reading and writing datasets to AWS S3 for StorageSession.
    """

    def __init__(self) -> None:
        super().__init__()

    def check_connection(self, connection_keys) -> bool:
        # implementation detail
        return True

    def getDataset(self, filename, connection_keys, bucket):
        # implementation detail
        pass

    def writeDataset(self, filename, data_object, connection_keys, bucket):
        parquetObj = self.parquetToObject(data_object)
