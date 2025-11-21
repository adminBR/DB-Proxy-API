from sqlalchemy import create_engine
from config import ORACLE_DB_PARAMS_PROD, ORACLE_DB_PARAMS_TEST, POSTGRES_DB_PARAMS
from sqlalchemy.ext.asyncio import create_async_engine


def get_oracle_connection(env="prod"):
    params = ORACLE_DB_PARAMS_PROD if env == "prod" else ORACLE_DB_PARAMS_TEST
    conn_str = (
        f"oracle+cx_oracle://{params['user']}:{params['password']}@{params['dsn']}"
    )
    return create_engine(conn_str)


async def get_oracle_connection_async(env="prod"):
    params = ORACLE_DB_PARAMS_PROD if env == "prod" else ORACLE_DB_PARAMS_TEST
    conn_str = (
        f"oracle+asyncoracledb://{params['user']}:{params['password']}@{params['dsn']}"
    )
    return create_async_engine(conn_str)


def get_postgres_connection(database_name: str):
    conn_str = (
        f"postgresql+psycopg2://{POSTGRES_DB_PARAMS['user']}:"
        f"{POSTGRES_DB_PARAMS['password']}@"
        f"{POSTGRES_DB_PARAMS['host']}:"
        f"{POSTGRES_DB_PARAMS['port']}/{database_name}"
    )
    return create_engine(conn_str)


async def get_postgres_connection_async(database_name: str):
    conn_str = (
        f"postgresql+asyncpg://{POSTGRES_DB_PARAMS['user']}:"
        f"{POSTGRES_DB_PARAMS['password']}@"
        f"{POSTGRES_DB_PARAMS['host']}:"
        f"{POSTGRES_DB_PARAMS['port']}/{database_name}"
    )
    return create_async_engine(conn_str)


def get_connection(database: str, database_name: str, env="prod"):
    if database.lower() == "oracle":
        return get_oracle_connection(env)
    if database.lower() == "postgres":
        return get_postgres_connection(database_name)
    raise ValueError(
        f"Unsupported database type '{database}', please use one of the active databases: 'oracle' or 'postgres'"
    )


async def get_connection_async(database: str, database_name: str, env="prod"):
    if database.lower() == "oracle":
        return await get_oracle_connection_async(env)
    if database.lower() == "postgres":
        return await get_postgres_connection_async(database_name)
    raise ValueError(
        f"Unsupported database type '{database}', please use one of the active databases: 'oracle' or 'postgres'"
    )
