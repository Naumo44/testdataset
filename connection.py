import pandas as pd 
import numpy as np 
from sqlalchemy import create_engine
from config import host, port, database, user, password, table_name

def read_table_sqlalchemy(host, port, database, user, password, table_name, schema='public'):
    # Создание строки подключения для SQLAlchemy
    db_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    # Создание движка SQLAlchemy
    db_engine = create_engine(db_string)
    # Использование pd.read_sql для загрузки таблицы в DataFrame
    # df = pd.read_sql(f"SELECT * FROM {schema}.{table_name}", con=db_engine)
    return db_engine

db_engine = read_table_sqlalchemy(host, port, database, user, password, table_name)