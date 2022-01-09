from Storage.Handlers import PROVIDERS


class StorageSession:
    def __init__(self) -> None:
        self.__provider = None
        self.__handler = None
        self.__connection_keys = None

    def __get_handler(self):
        """Get storage handler for selected provider from Handlers."""

        if self.__provider not in PROVIDERS.keys():
            print(
                f"{self.__provider} is not a supported provider. Supported Providers are {', '.join(PROVIDERS.keys())}"
            )
            return None

        return PROVIDERS[self.__provider]()

    def setStorage(self, provider, connection_keys):
        """Sets storage with given the given configuration if configuration is valid.

        Setting the storage succesfully assigns a appropriate handler to self based on
        the selected provider. This handler has getDataset and writeDataset methods for
        the selected provider and is called whenever getDataset or writeDataset is called.

        Args:
            provider:
            connection_keys:

        Returns:
            A string notifying if the connection succeeded or reason
            for the connection failure.

        """
        self.__provider = provider
        handler = self.__get_handler()

        if handler is None:
            return "Unrecognized provider"

        if handler.check_keys(connection_keys):
            self.__connection_keys = connection_keys
        else:
            return "Incorrect connection keys provided"

        if handler.check_connection(connection_keys):
            self.__handler = handler
            return "Connection Succeded"
        else:
            return "Connection Failed"

    def getDataset(self, filename, bucket="chainbraintest"):
        """Reads dataset from storage using filename and connection keys provided.
        Filename is used as key.

        Args:
            filename: A string by which the stored object is to be identified by.

        Returns:
            An arrow table object.

        """
        dataset = self.__handler.getDataset(filename, self.__connection_keys, bucket)
        return dataset

    def writeDataset(self, filename, data_object, bucket="chainbraintest"):
        """Writes arrow dataset to storage.

        Args:
            filename: A string by which the stored object is to be identified by.
            data_object: An arrow table object.

        Returns:
            String "Success" or "Failed".

        """
        success = self.__handler.writeDataset(
            filename, data_object, self.__connection_keys, bucket
        )
        if success:
            return "Success"
        else:
            return "Failed"
