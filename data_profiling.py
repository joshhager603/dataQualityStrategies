import pandas as pd
from constants import *
import common_functions

# pull the raw data from postgres
engine = common_functions.create_sqlalchemy_engine()
df = pd.read_sql(f'SELECT * FROM {RAW_DATA_TABLE_NAME}', engine)

