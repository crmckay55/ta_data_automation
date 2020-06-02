import  sys
import logging
import json
from bs4 import BeautifulSoup
import pandas as pd


def process_data(blob_contents, partition_key, row_prefix):
    
    version = _get_version(partition_key)
    logging.info(f'Processing version {version} of IW38')

    func = getattr(sys.modules[__name__], "_" + version)
    logging.info(f'Found processor version for {version}')

    return func(blob_contents, partition_key, row_prefix)
  

def _get_version(partition_key):
    # template:
    # IW38_01-Carseland 2021-20200523
    # 0000000-11111111111111-22222222
    # split(-)  split(-)     split(-)
    #  keep  
    return str(partition_key).split('-')[0]


'''---------------------------------------
 Version 1                                 
 Order is the first column, and we need to 
 pull the third value in the order column  
 to get the actual work order number       
----------------------------------------'''
def _IW38_01(blob_contents, partition_key, row_prefix):
    # processes version 1 of IW38
    # Doesn't matter how many columns.
    # Accounts for the weird thing happening in the first column

    df = pd.DataFrame(columns=['PartitionKey', 'RowKey', 'json_data'])

    # parse contents
    soup = BeautifulSoup(blob_contents, "html.parser")
    logging.info('Parsed blob item with BeautifulSoup')

    # strip out lists only
    stripped_lists = soup('table', {"class": "list"})
    logging.info('Stripped out the lists.')

    # iterate through lists to get bodies
    for idx_lst, lst in enumerate(stripped_lists):
        stripped_bodies = lst('tbody')

        # iterate through bodies to get rows
        for idx_body, body in enumerate(stripped_bodies):

            # if first list and body, get header rows
            if idx_lst == 0 and idx_body == 0:
                hdr_row = body('tr')
                columns = hdr_row[0]('td')
                keys = _IW38_01_set_headers(columns)

            # if not first list, skip body 0 which is the header again
            elif idx_body > 0:
                stripped_rows = body('tr')

                # iterate through rows and get columns
                for idx_row, row in enumerate(stripped_rows):
                    columns = row('td')
                    
                    df = df.append({'PartitionKey': partition_key,
                                    'RowKey': row_prefix + str(len(df) + 1),
                                    'json_data': _IW38_01_get_row(columns, keys)},
                                   ignore_index=True)

    logging.info('Finished parsing the file into a dataframe.')
    return df


def _IW38_01_set_headers(columns):
    '''Return headers as list'''

    header_list = []

    for column_idx, column in enumerate(columns):
        value = column('nobr')
        header_list.append(value[0].text.strip().replace('\xa0', ' '))

    return header_list


def _IW38_01_get_row(columns, headers):
    '''Return row of data as json'''

    current_row = {}

    for column_idx, column in enumerate(columns):
        value = column('nobr')
        if column_idx == 0:
            idx = 2
        else:
            idx = 0

        current_row[headers[column_idx]] = value[idx].text.strip().replace('\xa0', ' ')

    return json.dumps(current_row)


'''---------------------------------------
 Version 2                                 
 Same as version 1, but parses to a json
 file instead for gen 2 storage, not
 table storage       
----------------------------------------'''

'''
def _IW38_02(blob_contents, partition_key, row_prefix):
    # TODO: make this dump the columns into a normal dataframe format, and then dump the DF into a json itself with name of file the PartitionKey
    # processes version 1 of IW38
    # Doesn't matter how many columns.
    # Accounts for the weird thing happening in the first column

    df = pd.DataFrame(columns=['PartitionKey', 'RowKey', 'json_data'])

    # parse contents
    soup = BeautifulSoup(blob_contents, "html.parser")
    logging.info('Parsed blob item with BeautifulSoup')

    # strip out lists only
    stripped_lists = soup('table', {"class": "list"})
    logging.info('Stripped out the lists.')

    # iterate through lists to get bodies
    for idx_lst, lst in enumerate(stripped_lists):
        stripped_bodies = lst('tbody')

        # iterate through bodies to get rows
        for idx_body, body in enumerate(stripped_bodies):

            # if first list and body, get header rows
            if idx_lst == 0 and idx_body == 0:
                hdr_row = body('tr')
                columns = hdr_row[0]('td')
                keys = _IW38_01_set_headers(columns)

            # if not first list, skip body 0 which is the header again
            elif idx_body > 0:
                stripped_rows = body('tr')

                # iterate through rows and get columns
                for idx_row, row in enumerate(stripped_rows):
                    columns = row('td')

                    df = df.append({'PartitionKey': partition_key,
                                    'RowKey': row_prefix + str(len(df) + 1),
                                    'json_data': _IW38_01_get_row(columns, keys)},
                                   ignore_index=True)

    logging.info('Finished parsing the file into a dataframe.')
    return df
    '''