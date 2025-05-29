import numpy as np
import pandas as pd
from datetime import datetime, date
from datacleanser import clean_data

def test_clean_data():
    raw_data = [
        [np.int64(42), np.float64(3.14), None, pd.NA, datetime(2023, 5, 1), date(2023, 6, 1), "hello"]
    ]
    result = clean_data(raw_data)
    assert result[0][0] == 42
    assert result[0][1] == 3.14
    assert result[0][2] is None
    assert result[0][3] is None
    assert result[0][4] == "2023-05-01T00:00:00"
    assert result[0][5] == "2023-06-01"
    assert result[0][6] == "hello"
