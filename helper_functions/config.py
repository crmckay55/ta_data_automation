# Config for ta_data_automation function

import os


class DefaultConfig:

    STORAGE_CONNECTION = os.environ.get('AzureWebJobsStorage')

    OUTPUT_TABLE = os.environ.get('SnapshotDataTable')

    DIAGNOSTIC_TABLE = os.environ.get('SnapshotDiagnosticTable')

    BLOB_CONTAINER = os.environ.get('StorageBlobContainer')
