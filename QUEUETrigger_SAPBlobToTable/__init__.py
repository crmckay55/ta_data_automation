import logging
import json, uuid, os, sys
import traceback
from datetime import datetime
import azure.functions as func
from ..helper_functions import process_file as pf
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import EntityProperty, EdmType

# <TODO> make a config file for all environmental variables

def main(msg: func.QueueMessage) -> None:

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
    table = os.getenv('SnapshotDataTable')
    rowKey = str(uuid.uuid4())

    data = {
            'PartitionKey': blob_name,
            'RowKey': rowKey,
            'Records': records,
            'Status': status,
            'Error': error,
            'StartTime': str(start_time),
            'EndTime': str(end_time),
            'Duration': duration,
            'Table': table
        }
    cs = os.getenv('AzureWebJobsStorage')
    st = os.getenv('SnapshotDiagnosticTable')

    logging.info(f'Starting write to storage table {st}.')
    table_service = TableService(connection_string=cs)
    table_service.insert_or_replace_entity(st, data, timeout=None )