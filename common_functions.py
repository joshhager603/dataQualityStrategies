from constants import *
from sqlalchemy import Engine
import sqlalchemy

def create_sqlalchemy_engine(postgres_user=POSTGRES_USER, 
                             postgres_pass=POSTGRES_PASS, 
                             postgres_host=POSTGRES_HOST, 
                             postgres_port=POSTGRES_PORT, 
                             db_name=DB_NAME) -> Engine:
    return sqlalchemy.create_engine(
        f'postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}:{POSTGRES_PORT}/{DB_NAME}')