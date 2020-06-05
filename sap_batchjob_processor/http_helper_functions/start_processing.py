# ---------------------------------------------------------------
# author: Chris McKay
# version: 1.0
# date: June 4, 2020
# Environmental variables for the "dataautomation" app service plan in Azure.
# Primary storage (PS) is higher end for managing active data files.
# Secondary storage (SS) is for logs, queues, and archives.
# Changes:
# ---------------------------------------------------------------

import os, importlib, logging, tempfile

try:
    from . import manager_blobs
    from . import manager_tables
    from .. import azure_config
except ModuleNotFoundError:
    from sap_batchjob_processor.http_helper_functions import manager_blobs
    from sap_batchjob_processor.http_helper_functions import manager_tables
    from sap_batchjob_processor import azure_config


def process_data(filename: str):

    config = azure_config.DefaultConfig()

    source_blob = manager_blobs.BlobHandler(config.PS_CONNECTION,
                                            config.PS_RAW,
                                            '')

    destination_blob = manager_blobs.BlobHandler(config.PS_CONNECTION,
                                                 config.PS_INPROCESS,
                                                 '')

    log_table = manager_tables.TableHandler(config.SS_CONNECTION,
                                            config.SS_LOGSTABLE)

    file_parameters = _parse_filename(filename)
    in_process_blob = _create_in_process_filename(file_parameters)
    source_contents = source_blob.get_blob(filename)

    # TODO: figure out correct transaction to call.
    # TODO: import said module dynamically, allowing for local vs. azure imports
    # TODO: call transaction module dynamically with contents and version (e.g. IW38_01)
    # TODO: create in process filename

    # TODO: write data to destination blob.
    # TODO: add a paramater to specify how to write: json, CSV, delimiters etc.
    destination_blob.write_blob('')  # TODO: add dynamic filename.

    # TODO: write status to log table
    # TODO: need to catch exceptions, and still write to log table.

    # TODO return the temporary filename so it can be returned to the calling data factory for further processing.
    return in_process_blob


def _parse_filename(filename: str):
    parameters = {'file':filename}
    # template:
    # Job SAP-IW38_01-Carseland 2021-20200523, Step 1.htm <- update this!
    # TODO: template will change.  need standardized in SAP

    return parameters


def _create_in_process_filename(parsed_filename: dict):
    inprocess_filename = ''
    # TODO: rearrange the parsed filename into an inprocess filename to be saved and reused by data factory

    return inprocess_filename

