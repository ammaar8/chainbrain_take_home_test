from Storage.Handlers.StorageHandler_AWS import StorageHandler_AWS
from Storage.Handlers.StorageHandler_Azure import StorageHandler_Azure
from Storage.Handlers.StorageHandler_Minio import StorageHandler_Minio


PROVIDERS = {
    "aws": StorageHandler_AWS,
    "azure": StorageHandler_Azure,
    "minio": StorageHandler_Minio,
}
__all__ = ["PROVIDERS"]
