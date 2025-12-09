from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from util.db_connectors import get_db_engine, get_redis_client
from sqlalchemy import text
import asyncio
from time import time
from typing import Optional, Literal
import json
from pydantic import BaseModel, Field


router = APIRouter()

r = get_redis_client()


class QueryRequest(BaseModel):
    query: str = Field(
        ..., description="The SQL query to execute against the selected database."
    )
    database: str = Field(
        ..., description="The database type to use, such as 'oracle' or 'postgres'."
    )
    database_name: str = Field(
        ..., description="The target database name (unused for Oracle)."
    )
    cache_duration_seconds: Optional[int] = Field(
        None,
        ge=10,
        le=3600,
        description="Optional cache duration in seconds for Redis (between 10 and 3600).",
    )


class QueryResponse(BaseModel):
    cached: bool = Field(
        ..., description="Indicates whether the response was served from cache."
    )
    time_taken: float = Field(
        ..., description="Time in seconds that the execution took."
    )
    headers: list[str] = Field(..., description="Column headers returned by the query.")
    data: list[dict] = Field(..., description="Rows returned by the executed query.")


async def run_blocking_query(engine, query):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: engine.execute(query))


# Endpoint for query execution with improved I/O handling
@router.post("/run_query_async", response_model=QueryResponse)
async def run_query_async(request: QueryRequest):
    if request.query.strip() == "":
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    try:
        engine = get_db_engine(request.database, request.database_name)
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to acquire connection: {e}"
        )

    if request.cache_duration_seconds:
        cached_result = r.get(request.query)
        if cached_result and type(cached_result) == bytes:
            cached_data = json.loads(cached_result)
            return QueryResponse(
                cached=True,
                time_taken=0.0,
                headers=cached_data["headers"],
                data=cached_data["data"],
            )

    # Continuing with query execution without cache
    try:

        def execute_query():
            with engine.connect() as connection:
                result = connection.execute(text(request.query))
                rows = [dict(row) for row in result.mappings().all()]
                headers = list(rows[0].keys()) if rows else []
                return rows, headers

        loop = asyncio.get_running_loop()

        start_time = time()
        rows, headers = await loop.run_in_executor(None, execute_query)
        time_taken = time() - start_time

        if request.cache_duration_seconds:
            r.set(
                request.query,
                json.dumps(
                    {"headers": headers, "data": rows},
                    default=str,
                ),
                ex=request.cache_duration_seconds,
            )

        return QueryResponse(
            cached=False, time_taken=time_taken, headers=headers, data=rows
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {e}")


@router.post("/run_query", response_model=QueryResponse)
def run_query_basic(request: QueryRequest):
    """
    Execute a SQL query against the specified database.
    """
    if request.query.strip() == "":
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    try:
        engine = get_db_engine(request.database, request.database_name)
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to acquire connection: {e}"
        )

    if request.cache_duration_seconds:
        cached_result = r.get(request.query)
        if cached_result and type(cached_result) == bytes:
            cached_data = json.loads(cached_result)
            return QueryResponse(
                cached=True,
                time_taken=0.0,
                headers=cached_data["headers"],
                data=cached_data["data"],
            )
    # continuing with query execution without cache
    try:
        start_time = time()
        with engine.connect() as connection:
            result = connection.execute(text(request.query))
            rows = [dict(row) for row in result.mappings().all()]
            headers = list(rows[0].keys()) if rows else []
            time_taken = time() - start_time

            if request.cache_duration_seconds:
                r.set(
                    request.query,
                    json.dumps(
                        {"headers": headers, "data": rows},
                        default=str,
                    ),
                    ex=request.cache_duration_seconds,
                )

            response = QueryResponse(
                cached=False, time_taken=time_taken, headers=headers, data=rows
            )
            return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {e}")
