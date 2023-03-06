#!/usr/bin/env python

import os
import fire
import numpy as np
from sklearn import set_config
from loguru import logger
from basix import files, parquet

from src.data.load import get_links_by_types, download_file, extract_filename, extract_type, read_csv_within_zip
from src.config import PATH_ZIP_DIRECTORY, PATH_PARQUET_DIRECTORY
from src.data.load import add_date, get_current_timestamp
from src.utils import intclip

def cast_column(df, column, dtype):
    if column in df:
        df[column] = df[column].astype(dtype)
    return df


def format_columns(df):

    column_types = {
        'fax': 'string',
        'ddd_fax': 'string',
        'ddd1': 'string',
        'ddd1': 'string',
        'telefone1': 'string',
        'telefone2': 'string',
        'column_types': 'string',
        'cep': 'string',
    }

    for column in column_types:
        df = cast_column(df, column, column_types[column])

    return df

def main(part, datatype, update = False, batch_size = 1000000):

    update = update == 'True'
    batch_size = int(batch_size) if batch_size != '' else 1000000

    if datatype == "est":
        database_type = "Estabelecimentos"

    elif datatype == "emp":
        
        database_type = "Empresas"

    else:
        raise Exception(f"Not recognized datatype {datatype}")

    timestamp = get_current_timestamp()

    urls = get_links_by_types()

    if part >= len(urls[database_type]):
        message = f"Database has only {len(urls[database_type])} parts, but you passed part = {part}"
        logger.error(message)
        raise ValueError(message)

    if part < 0:
        message = "Negative values are not allowed."
        logger.error(message)
        raise Exception(message)

    url = urls[database_type][part]

    filename = extract_filename(url)

    zip_directory_path = os.path.join(PATH_ZIP_DIRECTORY, database_type)

    zip_local_filepath = os.path.join(zip_directory_path, filename)

    parquet_directory_path = os.path.join(PATH_PARQUET_DIRECTORY, database_type)

    parquet_local_filepath = os.path.join(parquet_directory_path, filename.replace(".zip", ".parquet"))

    files.make_directory(zip_directory_path)

    if update:
        logger.info(f"Downloading zip from {url} and persisting to {zip_local_filepath}")
        download_file(url, zip_local_filepath)
        
    try:

        filename = extract_filename(url)

        logger.info(f'---------------------- {filename.replace(".zip", "")}')

        zip_directory_path = os.path.join(PATH_ZIP_DIRECTORY, database_type)

        zip_local_filepath = os.path.join(zip_directory_path, filename)

        parquet_directory_path = os.path.join(PATH_PARQUET_DIRECTORY, database_type)

        parquet_local_filepath = os.path.join(parquet_directory_path, filename.replace(".zip", ".parquet"))

        files.make_directory(zip_directory_path)

        if update:
            logger.info(f"Downloading zip from {url} and persisting to {zip_local_filepath}")
            download_file(url, zip_local_filepath)

        logger.info(f"Reading csv within {zip_local_filepath}")
        df = read_csv_within_zip(zip_local_filepath)

        df = df.pipe(add_date, timestamp = timestamp).pipe(format_columns)

        size = len(df)

        n_parts = size // batch_size + int(size % batch_size > 0)

        for i in range(n_parts):
            start = i * batch_size
            end = intclip((1+i) * batch_size, 0, size)
            temp = df.iloc[start:end]
            local = parquet_local_filepath.replace(".parquet", f"_part_{str(i+1).zfill(2)}.parquet")
            logger.info(f"Persisting the part {i+1} of dataframe to parquet format in {local}")
            parquet.write(temp, local, overwrite=True)

    except Exception as err:
        logger.error(err)


if __name__ == '__main__':
    fire.Fire(main)