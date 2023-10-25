import time
import sqlalchemy
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd


def execution_time_counter(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Время выполнения функции: {time.strftime('%H:%M:%S', time.gmtime(execution_time))}")
        return result
    return wrapper


class DB:
    def __init__(self, host, username, password, database):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}/{database}")

    @execution_time_counter
    def create_table(self, df, schema, table):
        time.sleep(3)

        inspector = inspect(self.engine)

        if table not in inspector.get_table_names(schema=schema):
            df.to_sql(table, self.engine, schema=schema, if_exists='append', index=False)
        else:
            print(f'Таблица "{table}" уже существует в базе данных.')


    @execution_time_counter
    def delete_from_table(self, table, schema, conditions):
        query = f"DELETE FROM {schema}.{table} WHERE {conditions}"
        inspector = inspect(self.engine)

        if table not in inspector.get_table_names(schema=schema):
            print(f'Таблицы "{table}" не существует в базе данных.')
        else:
            with self.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()


    @execution_time_counter
    def truncate_table(self, schema, table):
        query = f"TRUNCATE TABLE {schema}.{table}"
        inspector = inspect(self.engine)

        if table not in inspector.get_table_names(schema=schema):
            print(f'Таблицы "{table}" не существует в базе данных.')
        else:
            with self.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()

    @execution_time_counter
    def read_sql(self, query):
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_query(text(query), conn)
                return df
        except sqlalchemy.exc.ProgrammingError as e:
            print(e)


    @execution_time_counter
    def insert_sql(self, df, schema, table):
        inspector = inspect(self.engine)

        if table not in inspector.get_table_names(schema=schema):
            print(f'Таблицы "{table}" не существует в базе данных.')
        else:
            df.to_sql(table, self.engine, schema=schema, if_exists='append', index=False)

    @execution_time_counter
    def execute(self, query):
        with self.engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()
