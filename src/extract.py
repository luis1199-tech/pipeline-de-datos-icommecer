from typing import Dict

import requests
from pandas import DataFrame, read_csv, read_json, to_datetime

def temp() -> DataFrame:
    """Get the temperature data.
    Returns:
        DataFrame: A dataframe with the temperature data.
    """
    return read_csv("data/temperature.csv")

# funcion para la extraccion

def get_public_holidays(public_holidays_url: str, year: str) -> DataFrame:
    """Get the public holidays for the given year for Brazil.
    Args:
        public_holidays_url (str): URL base para obtener los días festivos.
        year (str): El año para obtener los días festivos.
    Raises:
        SystemExit: Si la solicitud HTTP falla.
    Returns:
        DataFrame: Un dataframe con los días festivos públicos.
    """
    # Construir la URL: public_holidays_url/{year}/BR
    url = f"{public_holidays_url}/{year}/BR"
    
    # Realizar la solicitud GET
    response = requests.get(url)
    
    # Si la solicitud falla, raise_for_status() lanzará una excepción HTTPError;
    # capturamos la excepción y lanzamos SystemExit.
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise SystemExit(e)
    
    # Convertir la respuesta JSON en un DataFrame
    data = response.json()
    df = DataFrame(data)
    
    # Eliminar las columnas "types" y "counties" si existen
    df.drop(columns=["types", "counties"], inplace=True, errors="ignore")
    
    # Convertir la columna "date" a formato datetime
    df["date"] = to_datetime(df["date"])
    
    return df



def extract(
    csv_folder: str, csv_table_mapping: Dict[str, str], public_holidays_url: str
) -> Dict[str, DataFrame]:
    """Extract the data from the csv files and load them into the dataframes.
    Args:
        csv_folder (str): The path to the csv's folder.
        csv_table_mapping (Dict[str, str]): The mapping of the csv file names to the
        table names.
        public_holidays_url (str): The url to the public holidays.
    Returns:
        Dict[str, DataFrame]: A dictionary with keys as the table names and values as
        the dataframes.
    """
    dataframes = {
        table_name: read_csv(f"{csv_folder}/{csv_file}")
        for csv_file, table_name in csv_table_mapping.items()
    }

    holidays = get_public_holidays(public_holidays_url, "2017")

    dataframes["public_holidays"] = holidays

    return dataframes
