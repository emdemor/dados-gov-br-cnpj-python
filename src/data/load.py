import os
import re
import pandas as pd
import requests
import zipfile
from basix import files
from bs4 import BeautifulSoup
from loguru import logger
from typing import Dict
from typing import Generator
from typing import List
from typing import Union
from ..config import BASE_URL
from ..config import DATABASE_TYPES
from ..config import PATH_ZIP_DIRECTORY
from ..config import DATABASE_COLUMNS


def get_links() -> Generator[str, None, None]:
    """
    This function retrieves all links from a webpage that end with .zip and
    returns them as a list of strings.
    """
    page = requests.get(BASE_URL)

    soup = BeautifulSoup(page.text)

    for link in soup.find_all("a"):

        if str(link.get("href")).endswith(".zip"):

            filename = link.get("href")

            if not filename.startswith("http"):
                yield BASE_URL + filename

            else:
                yield filename


def get_links_by_types() -> Dict[str, List[str]]:
    """
    This function retrieves all links from a webpage that end with .zip and
    groups them based on the type specified in DATABASE_TYPES.

    Returns:
        Dict[str, List[str]]: a dictionary where each key represents a type
        from DATABASE_TYPES and its corresponding value is a list of strings
        representing the links that contain that type.
    """

    file_links = list(get_links())

    return {type: list(filter(lambda x: type in x, file_links)) for type in DATABASE_TYPES}


def extract_filename(url: str) -> str:
    """
    This function extracts the filename from a given URL.
    Args:
        url (str): the URL to extract the filename from.

    Returns:
        str: the extracted filename.
    """
    return re.search("([^/]+$)", url).group(0)


def extract_type(url: str) -> str:
    """
    This function extracts the database type from a given URL.
    Args:
        url (str): the URL to extract the filename from.

    Returns:
        str: the extracted filename.
    """
    filename = extract_filename(url)
    return "".join(filter(lambda x: not x.isdigit(), filename.replace(".zip", "")))


def download_file(url: str, path: str) -> None:
    """
    This function downloads a file from the specified URL and saves it to the specified path.
    Args:
        url (str): the URL of the file to download.
        path (str): the path to save the file to.

    Raises:
        Exception: if there is an error while downloading the file.
    """
    try:
        response = requests.get(url)
        with open(path, "wb") as file:
            file.write(response.content)

    except Exception as err:
        logger.error(err)
        raise err


def download_zip_file(url: str) -> Union[None, Exception]:
    """
    This function downloads a zip file from a given URL.
    Args:
        url (str): the URL to download the zip file from.

    Returns:
        Union[None, Exception]: None if the download was successful or an exception if the download failed.
    """
    assert url[-4:] == ".zip", "The url do not poit to a zip file"

    try:
        logger.info(f"Download zip file from {url}")
        filename = extract_filename(url)
        database_type = extract_type(url)
        directory_path = os.path.join(PATH_ZIP_DIRECTORY, database_type)
        local_filepath = os.path.join(directory_path, filename)
        files.make_directory(directory_path)
        logger.info(f"Download zip file to {local_filepath}")
        download_file(url, local_filepath)

    except Exception as err:
        logger.error(err)
        raise err


def read_csv_within_zip(zip_filepath: str) -> pd.DataFrame:
    """
    This function reads all csv files within a zip file and concatenates them into a single dataframe.

    Args:
        zip_filepath (str): the file path to the zip file.

    Returns:
        pd.DataFrame: a dataframe containing the concatenated data from all csv files within the zip file.
    """

    zf = zipfile.ZipFile(zip_filepath)

    database_type = os.path.dirname(zip_filepath).split("/")[-1]

    pdread_opts = dict(sep=";", encoding="latin-1", header=None)

    dataframe = pd.DataFrame()

    for filename in zf.namelist():
        try:
            temp = pd.read_csv(zf.open(filename), **pdread_opts)
            temp.columns = DATABASE_COLUMNS[database_type]
            dataframe = pd.concat([dataframe, temp])

        except Exception as err:
            logger.warning(
                f"It was not possible to read the file {filename}"
                f"within de zip file {zip_filepath}"
            )

    return dataframe
