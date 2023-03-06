#!/usr/bin/env python

import os
import fire
import numpy as np
import pandas as pd
from sklearn import set_config
from loguru import logger
from basix import files, parquet

from src.data.load import get_links_by_types, download_file, extract_filename, extract_type, read_csv_within_zip
from src.config import PATH_ZIP_DIRECTORY, PATH_PARQUET_DIRECTORY
from src.data.load import add_date, get_current_timestamp
from src.utils import intclip

def create_cnpj(df):
    df['cnpj'] = df['cnpj_base'].astype(str).str.zfill(7) + df['cnpj_ordem'].astype(str).str.zfill(4) + df['cnpj_dv'].astype(str).str.zfill(2)
    return df


def cast_column(df, column, dtype):
    if column in df:
        df[column] = df[column].astype(dtype)
    return df


def format_columns(df, column_types):

    for column in column_types:
        df = cast_column(df, column, column_types[column])

    return df

def select_columns(df, columns):
    return df[columns]


def main(est, emp, lab):
    
    df_est = pd.read_parquet(est)
    df_emp = pd.read_parquet(emp)
    
    merged = (
        df_est.merge(df_emp, on="cnpj_base", how="inner", suffixes=("", "_duplicated"))
        .pipe(create_cnpj)
    )

    local = f"datasets/interim/parquet/Merged/{lab}.parquet"

    parquet.write(merged, local, overwrite=True)


def get_names(est, emp, lab):
    
    df_est = pd.read_parquet(est)
    df_emp = pd.read_parquet(emp)


    column_types = {
        'cnpj': 'string',
        'nome_fantasia': 'string',
        'razao_social': 'string',
    }
    
    merged = (
        df_est.merge(df_emp, on="cnpj_base", how="inner", suffixes=("", "_duplicated"))
        .pipe(create_cnpj)
        .pipe(select_columns, columns = list(column_types.keys()))
        .pipe(format_columns, column_types=column_types)
    )

    local = f"datasets/interim/parquet/Names/part-{str(lab).zfill(6)}.parquet"

    parquet.write(merged, local, overwrite=True)


def get_summary(est, emp, lab):
    
    df_est = pd.read_parquet(est)
    df_emp = pd.read_parquet(emp)


    column_types = {
        'cnpj': 'string',
        'nome_fantasia': 'string',
        'razao_social': 'string',
        'cd_cnae_principal': 'string',
        'porte_empresa': "Int32",
        'natureza_juridica': "Int32",
        'capital_social': "string",
    }
    
    merged = (
        df_est.merge(df_emp, on="cnpj_base", how="inner", suffixes=("", "_duplicated"))
        .pipe(create_cnpj)
        .pipe(select_columns, columns = list(column_types.keys()))
        .pipe(format_columns, column_types=column_types)
    )

    local = f"datasets/interim/parquet/SummaryInfo/part-{str(lab).zfill(6)}.parquet"

    parquet.write(merged, local, overwrite=True)


if __name__ == '__main__':
    #fire.Fire(main)
    fire.Fire(get_names)