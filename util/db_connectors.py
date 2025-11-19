from sqlalchemy import create_engine
from config import ORACLE_DB_PARAMS_PROD, ORACLE_DB_PARAMS_TEST, POSTGRES_DB_PARAMS


def get_oracle_connection(env="prod"):
    try:
        ORACLE_DB_PARAMS = (
            ORACLE_DB_PARAMS_PROD if env == "prod" else ORACLE_DB_PARAMS_TEST
        )
        engine = create_engine(
            f'oracle+cx_oracle://{ORACLE_DB_PARAMS["user"]}:{ORACLE_DB_PARAMS["password"]}@{ORACLE_DB_PARAMS["dsn"]}'
        )
        return engine
    except Exception as e:
        print(e)
        raise Exception(e)


def get_postgres_connection():
    try:
        engine = create_engine(
            f'postgresql+psycopg2://{POSTGRES_DB_PARAMS["user"]}:{POSTGRES_DB_PARAMS["password"]}@{POSTGRES_DB_PARAMS["host"]}:{POSTGRES_DB_PARAMS["port"]}/{POSTGRES_DB_PARAMS["db_name"]}'
        )
        return engine
    except Exception as e:
        print(e)
        raise Exception(e)


def get_connection(database: str, env="prod"):
    if database.lower() == "oracle":
        return get_oracle_connection(env)
    elif database.lower() == "postgres":
        return get_postgres_connection()
    else:
        raise ValueError("Unsupported database type")
