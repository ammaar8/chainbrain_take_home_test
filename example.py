import pyarrow as pa
import pyarrow.parquet as pq
from Storage import StorageSession
from minio import Minio

# ----------------------------------------------------------------------------
# In this example we'll read a birthdays dataset "birthdays.parquet" from
# minio storage and process it so that it only contains records where date year
# is more than or equal to 2000 and write it back to storage
# as "processed_birthdays.parquet".
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Create an arrow table to use as dummy dataset if not present on storage
# days = pa.array([1, 12, 17, 23, 28], type=pa.int8())
# months = pa.array([1, 3, 5, 7, 1], type=pa.int8())
# years = pa.array([1990, 2000, 1995, 2000, 1995], type=pa.int16())
# birthdays_table = pa.table([days, months, years], names=["days", "months", "years"])
# ----------------------------------------------------------------------------

# Create a dictionary of keys with required paramters to make connection to minio.
# Required keys for minio are endpoint, access_key, secret_key.
# for making a connection locally, have to provide an extra key secure=False
connection_keys_minio = dict(
    endpoint="",
    access_key="",
    secret_key="",
    secure=False,  # For local connection
)

# Create a StorageSession object to use for reading and writing datasets
storage = StorageSession()

# Set storage
storage.setStorage("minio", connection_keys_minio)

# Get dataset
birthdays_table = storage.getDataset("birthdays.parquet")
print(" ------------ Read Arrow Table ---------------")
print(birthdays_table)

# Convert to pandas df
birthdays_df = birthdays_table.to_pandas()
print(" ------------ Read DataFrame ---------------")
print(birthdays_df)

# Filter dataset
processed_birthdays_df = birthdays_df[birthdays_df["years"] >= 2000]
print(" ------------ Processed DataFrame ---------------")
print(processed_birthdays_df)

# write dataset back to storage
processed_birthdays_table = pa.Table.from_pandas(processed_birthdays_df)
print(" ------------ Processed Dataset as Arrow Table ---------------")
print(processed_birthdays_table)
storage.writeDataset("processed_birthdays.parquet", processed_birthdays_table)

# read processed dataset from storage
print(" ------------ Processed Dataset Read From Storage---------------")
print(storage.getDataset("processed_birthdays.parquet"))
