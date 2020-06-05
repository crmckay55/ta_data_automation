import os, importlib
import logging
import tempfile

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
try:
    from . import storage_table
    from . import config
except:
    from helper_functions import storage_table
    from helper_functions import config


def process_blob(blob_name):

    az_config = config.DefaultConfig()

    logging.info(f'Started process_blob with {blob_name}.')

    partition_key = _get_partition_key(blob_name)
    row_prefix = _get_row_key_prefix(blob_name)
    file_name = blob_name
    logging.info(f'Set PartitionKey to {partition_key}.')

    blob_services_client = BlobServiceClient.from_connection_string(az_config.STORAGE_CONNECTION)
    blob_container_client = blob_services_client.get_container_client(az_config.BLOB_CONTAINER)
    blob_client = blob_container_client.get_blob_client(file_name)
    logging.info(f'created blob_client for {file_name}')

    # load new blob into an object
    temp_file_path = tempfile.gettempdir()
    temp_file = os.path.join(temp_file_path, 'file.htm')
    logging.info(f'Ready to save to {temp_file}')

    with open(temp_file, 'wb') as f:
        f.write(blob_client.download_blob().readall())
    logging.info(f'Saved to local file: {temp_file}')

    with open(temp_file, 'r') as f:  # try rb
        contents = f.read()
    logging.info(f'Read local file with length {len(contents)}')

    # <TODO> delete temporary file!!

    # dynamically call function based on transaction name
    # version will be handled within the function 
    # <TODO> write an inheritable class for all functions to implement.
    transaction = _get_transaction(blob_name)
    
    try:
        transaction_processor = importlib.import_module(f'helper_functions.tx_{transaction}', package=None)
        logging.info(f'Found module for {transaction} without __app__')
    except ModuleNotFoundError:
        transaction_processor = importlib.import_module(f'__app__.helper_functions.{transaction}', package=None)
        logging.info(f'Found module for {transaction} with __app__')

    parsed_df = transaction_processor.process_data(contents, partition_key, row_prefix)
    
    # TODO: Remove this commented line after debug
    # storage_table.update_storage_table(parsed_df, partition_key, row_prefix)
    storage_table.update_storage_blob(parsed_df, 'test.csv')

    # _delete_blob(blob_name, blob_container_client)

    return len(parsed_df)


def _delete_blob(blob_name, blob_container_client):
    blob_container_client.delete_blob(blob_name)
    logging.info(f'Deleted blob {blob_name}')


def _get_partition_key(blob_name):

    # template:
    # Job SAP-IW38_01-Carseland 2021-20200523, Step 1.htm
    # xxxxxxx-0000000-11111111111111-22222222,xxxxxxxxxxx
    #  strip  split(-)  split(-)      split(-)  split(,)
    #           keep    keep            keep                             

    return str(blob_name).strip('Job SAP-').split(',')[0]


def _get_row_key_prefix(blob_name):

    # template:
    # Job SAP-IW38_01-Carseland 2021-20200523, Step 1.htm
    # no row key prefix for now

    return ''


def _get_transaction(blob_name):
    # template:
    # Job SAP-IW38_01-Carseland 2021-20200523, Step 1.htm
    # xxxxxxx-0000000-11111111111111-22222222222222222222
    #  strip  split(-)  split(-)       split(-)  
    #         keep[0]        
    #         split(-)                     
    #          keep[0]

    return str(blob_name).strip('Job SAP-').split('-')[0].split('_')[0]
