# Chainbrain Take Home Test - Ammaar

# Storage

This package allows connecting to minio, azure and aws to read and write datasets from and to storage.

Example usage in example.py

# Methods

## setStorage(provider, connection_keys)

Sets the details required to connect, read and write to storage.

Example usage -

```
from Storage import StorageSession

storage = StorageSession()
storage.setStorage(
    'minio',
    {
        endpoint="ENDPOINT",
        access_key="ACCESSKEY",
        secret_key="SECRETACCESSKEY",
    }
)
```

## writeDataset(filename, data_object, bucket)

Write a arrow table object to storage.

Example Usage -

```
days = pa.array([1, 12, 17, 23, 28], type=pa.int8())
months = pa.array([1, 3, 5, 7, 1], type=pa.int8())
years = pa.array([1990, 2000, 1995, 2000, 1995], type=pa.int16())
birthdays_table = pa.table([days, months, years], names=["days", "months", "years"])

storage.writeDataset('birthdays.parquet', birthdays_table, 'chainbraintest')
```

## getDataset(filename, bucket)

Gets dataset 'filename' from storage bucket - 'bucket'.

Example usage -

```
birthdays_table = storage.getDataset('birthdays.parquet', 'chainbraintest')
```
