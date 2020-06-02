import logging
import uuid
import traceback
from datetime import datetime
import azure.functions as func

from azure.cosmosdb.table.tableservice import TableService

# first is import in Azure, second is Local import
try:
    from ..helper_functions import config
    from ..helper_functions import process_file as pf
except:
    from helper_functions import config
    from helper_functions import process_file as pf


# <TODO> make a config.py file for all environmental variables

def main(msg: func.QueueMessage) -> None:

    az_config = config.DefaultConfig()

    blob_name = msg.get_body().decode('utf-8')

    logging.info('Python queue trigger function processed a queue item: %s',
                 blob_name)

    start_time = datetime.utcnow()
    

    try:
        records = pf.process_blob(blob_name)
        status = 'Success'
        error = ''
        
    except Exception as e:
        traceback_str = ''.join(traceback.format_tb(e.__traceback__))
        error = str(e) + ": " +  traceback_str
        status = 'Failed'
        records = 0

    # write record in diagnostic table
    end_time = datetime.utcnow()
    duration = str(end_time - start_time)
    row_key = str(uuid.uuid4())

    data = {
            'PartitionKey': blob_name,
            'RowKey': row_key,
            'Records': records,
            'Status': status,
            'Error': error,
            'StartTime': str(start_time),
            'EndTime': str(end_time),
            'Duration': duration,
            'Table': az_config.OUTPUT_TABLE
        }

    logging.info(f'Starting write to storage table {az_config.DIAGNOSTIC_TABLE}.')
    table_service = TableService(connection_string=az_config.STORAGE_CONNECTION)
    table_service.insert_or_replace_entity(az_config.DIAGNOSTIC_TABLE, data, timeout=None )