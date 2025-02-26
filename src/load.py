from typing import Dict

from pandas import DataFrame
from sqlalchemy.engine.base import Engine


def load(data_frames: Dict[str, DataFrame], database: Engine):
    """Load the dataframes into the sqlite database.

    Args:
        data_frames (Dict[str, DataFrame]): Un diccionario con claves como los nombres de las tablas
        y valores como los dataframes a cargar.
    """
    for table_name, df in data_frames.items():
        # Cargar cada DataFrame en la base de datos SQLite.
        # Se usa if_exists="replace" para sobrescribir la tabla si ya existe.
        df.to_sql(name=table_name, con=database, index=False, if_exists="replace")
    raise NotImplementedError
