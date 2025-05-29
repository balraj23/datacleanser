from typing import Any
import psycopg2
import numpy as np
import pandas
from datetime import datetime, date

def clean_data(data: list) -> list[list[Any]]:
    return [
        [
            int(x)
            if isinstance(x, np.int64)
            else float(x)
            if isinstance(x, (np.float64, float)) and not np.isnan(x)
            else x.isoformat()
            if isinstance(x, datetime)
            else x.isoformat()
            if isinstance(x, date)
            else None
            if pandas.isna(x)
            else str(x)
            for x in row
        ]
        for row in data
    ]
    

def create_postgres_connection():
    pass