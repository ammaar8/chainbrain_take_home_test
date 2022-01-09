from io import BytesIO
import pyarrow.parquet as pq
import pyarrow as pa


class StorageHandler:
    """
    Base class for StorageHandlers for different storage providers.
    """

    def objectToParquet(self, data_object) -> BytesIO:
        """
        Converts arrow table object to parquet file for writing to storage.

        Args:
            data_object: An arrow table object.

        Returns:
            BytesIO() object in parquet format.
        """
        file = BytesIO()
        pq.write_table(data_object, file)
        return file

    def parquetToObject(self, file) -> pa.Table:
        """
        Converts parquet file to arrow table object to be consumed after reading from storage.

        Args:
            file: Parquet file.

        Returns:
            Arrow table object.

        """
        data_object = pq.read_table(BytesIO(file))
        return data_object

    def check_keys(self, required_keys, connection_keys) -> bool:
        """
        Checks if all required keys are present in keys provided.

        Args:
            required_keys: List of strings which are required for provider.
            connection_keys: List of strings provided to the Handler.

        Return:
            True or False.
        """
        provided_keys = connection_keys.keys()
        for k in required_keys:
            if k not in provided_keys:
                return False
            else:
                return True
