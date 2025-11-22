from sqlalchemy import create_engine
from config import ORACLE_DB_PARAMS_PROD, ORACLE_DB_PARAMS_TEST, POSTGRES_DB_PARAMS
from sqlalchemy.ext.asyncio import create_async_engine
import redis


def get_oracle_engine(env="prod"):
    params = ORACLE_DB_PARAMS_PROD if env == "prod" else ORACLE_DB_PARAMS_TEST
    conn_str = (
        f"oracle+oracledb://{params['user']}:{params['password']}@{params['dsn']}"
    )
    return create_engine(conn_str)


def get_postgres_engine(database_name: str):
    conn_str = (
        f"postgresql+psycopg2://{POSTGRES_DB_PARAMS['user']}:"
        f"{POSTGRES_DB_PARAMS['password']}@"
        f"{POSTGRES_DB_PARAMS['host']}:"
        f"{POSTGRES_DB_PARAMS['port']}/{database_name}"
    )
    return create_engine(conn_str)


def get_db_engine(database: str, database_name: str, env="prod"):
    if database.lower() == "oracle":
        return get_oracle_engine(env)
    if database.lower() == "postgres":
        return get_postgres_engine(database_name)
    raise ValueError(
        f"Unsupported database type '{database}', please use one of the active databases: 'oracle' or 'postgres'"
    )


def get_redis_client(host="192.168.1.16", port=6380, db=0):
    return redis.Redis(host=host, port=port, db=db)
