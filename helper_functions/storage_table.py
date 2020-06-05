import logging
import pandas as pd
from azure.cosmosdb.table.tableservice import TableService, TableBatch
from azure.cosmosdb.table.models import EntityProperty, EdmType
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

try:
    from . import config
except:
    from helper_functions import config


def update_storage_table(df, partition_key, row_prefix):

    az_config = config.DefaultConfig()

    table_service = TableService(connection_string=az_config.STORAGE_CONNECTION)
    logging.info(f'Starting write to storage table.')
   
    # check for duplicates.  if found, delete them.
    _check_duplicates_(table_service, az_config.OUTPUT_TABLE, partition_key, row_prefix)

    # <TODO> change return type so information can be logged if there's an error.
    _update_table_(df, table_service, az_config)


def update_storage_blob(df, filename):
    az_config = config.DefaultConfig()

    blob_services_client = BlobServiceClient.from_connection_string(az_config.STORAGE_CONNECTION)
    blob_container_client = blob_services_client.get_container_client(az_config.BLOB_CONTAINER)
    blob_client = blob_container_client.get_blob_client(filename)

    output = df.to_csv(index=False, encoding="utf-8", sep='\t')

    blob_client.upload_blob(output)



def _check_duplicates_(table_service, st, partition_key, row_prefix):

    duplicates = table_service.query_entities(st,
                                          filter=f"PartitionKey eq \'{partition_key}\'",
                                          num_results=1000)
    done = False
    total_records = 0

    logging.info(f'Checking for duplicates in {partition_key}')

    if len(list(duplicates)) > 0:

        while not done:

            # keep track of total records deleted
            total_records = total_records + len(list(duplicates))

            duplicates_length = len(list(duplicates))

            # if there's a marker, get it from the original query
            current_marker = getattr(duplicates, 'next_marker')
            batch = TableBatch()

            for idx, item in enumerate(duplicates):

                batch.delete_entity(item['PartitionKey'], item['RowKey'])

                # if we've reached either 100, or the length of dataset if < 100, commit batch
                if divmod(idx + 1, 100)[1] == 0 or idx + 1 == duplicates_length:
                    table_service.commit_batch(st, batch)
                    batch = TableBatch()
            
            if current_marker:
                duplicates = table_service.query_entities(st,
                                                    filter=f"PartitionKey eq \'{partition_key}\'",
                                                    num_results=1000, marker=current_marker)
            else:
                done = True

        logging.info(f'Found and deleted {str(total_records)} records with partition key {partition_key}')

    else:
    
        logging.info(f'Found no duplicates in {partition_key}')         

def _update_table_(df, service, az_config):

    logging.info(f'Starting write to storage table {az_config.OUTPUT_TABLE}.')

    batch_counter = 0
    batch = TableBatch()

    for idx, row in df.iterrows():
        task = {'PartitionKey':  row['PartitionKey'],
                'RowKey': str(row['RowKey']),
                'json_data': EntityProperty(EdmType.STRING, row['json_data'])}
        batch_counter += 1
        batch.insert_entity(task)

        if batch_counter == 100:
            batch_counter = 0
            service.commit_batch(az_config.OUTPUT_TABLE, batch)
            batch = None
            batch = TableBatch()

    if batch_counter > 0:
        service.commit_batch(az_config.OUTPUT_TABLE, batch)

    logging.info('Finished write to storage table.')
