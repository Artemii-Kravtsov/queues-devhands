import asyncpg
from pandas import DataFrame
from typing import List, Optional, Type
from collections import deque, namedtuple
from contextlib import asynccontextmanager


class PostgreSQL:
    pool = None

    def __init__(
            self, 
            user: str, 
            password: str, 
            database: str,
            host: str, 
            port: int) -> None:
        
        self.conn_params = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database}

    async def connect(self):
        self.pool = await asyncpg.create_pool(**self.conn_params)
        return self

    @asynccontextmanager
    async def transaction(self):
        async with self.pool.acquire() as connection:
            # если ошибок не было, при закрытии контекстного менеджера
            # asyncpg сделает commit. если ошибка, то rollback
            async with connection.transaction():
                yield connection

    async def execute(
            self,
            query: str,
            args: tuple = (),
            con: Optional[Type[asyncpg.connection.Connection]] = None):
        if con:
            await con.execute(query, *args)
            return ""
        async with self.pool.acquire() as con:
            await con.execute(query, *args)
        return ""
    
    async def select(
            self,
            query: str,
            args: tuple = (),
            con: Optional[Type[asyncpg.connection.Connection]] = None):
        if con:
            records = await con.fetch(query, *args)
        else:
            async with self.pool.acquire() as con:
                records = await con.fetch(query, *args)
        if not records:
            async with self.pool.acquire() as temp_con:
                stmt = await temp_con.prepare(query)
                columns = [col.name for col in stmt.get_attributes()]
            return DataFrame([], columns=columns)
        return DataFrame(records, columns=records[0].keys())

    async def insert(
            self,
            query: str,
            data: list,
            con: Optional[Type[asyncpg.connection.Connection]] = None) -> None:
        template = " (" + ", ".join(map(lambda x: f"${x}", range(1, len(data) + 1))) + ") "
        ins_place = query.lower().find("values") + len("values")
        query = query[:ins_place] + template + query[ins_place:]
        if con:
            await con.execute(query, *data)
            return
        async with self.pool.acquire() as con:
            await con.execute(query, *data)

    async def insert_many(
            self,
            query: str,
            data: List[list],
            con: Optional[Type[asyncpg.connection.Connection]] = None) -> None:
        
        template = " (" + ", ".join(map(lambda x: f"${x}", range(1, len(data[0]) + 1))) + ") "
        ins_place = query.lower().find("values") + len("values")
        query = query[:ins_place] + template + query[ins_place:]
        if con:
            await con.executemany(query, data)
            return
        async with self.pool.acquire() as con:
            await con.executemany(query, data)
