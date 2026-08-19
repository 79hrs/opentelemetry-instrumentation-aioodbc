import asyncio
import pyodbc
from aioodbc import Cursor, Connection
from opentelemetry.instrumentation.aioodbc.package import _instruments
from opentelemetry.instrumentation.aioodbc.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.semconv.attributes.db_attributes import DbSystemNameValues
from opentelemetry.trace import get_tracer, SpanKind
from opentelemetry.trace.status import Status, StatusCode
from typing import Any, Collection

_DBMS_NAME_MAP = {
    "Microsoft SQL Server": DbSystemNameValues.MICROSOFT_SQL_SERVER.value
}

def _parse_dsn(dsn: str) -> dict:
    parsed = {}
    for i in dsn.split(';'):
        if not i:
            continue
        k, v = i.split('=')
        parsed[k.lower()] = v

    host, port = parsed.get('server', '').split(',')
    return {
        "db.system.name": parsed.get('driver', 'Unidentified SQL Server'),
        "server.address": host,
        "server.port": port,
        "db.namespace": parsed.get('database', ''),
        "db.user": parsed.get('uid', '')
    }

async def _cache_db_attributes(conn, capture_db_user):
    _dsn_dict = _parse_dsn(conn.__dict__['_dsn'])
    _otel_db_attributes = {**_dsn_dict}

    db_name = await conn.getinfo(pyodbc.SQL_DBMS_NAME)
    if db_name:
        driver = _dsn_dict['db.system.name']
        _otel_db_attributes['db.system.name'] = _DBMS_NAME_MAP.get(db_name, driver)

    if not capture_db_user:
        _otel_db_attributes.pop('db.user', None)

    setattr(conn, '_otel_db_attributes', _otel_db_attributes)

class AioodbcInstrumentor(BaseInstrumentor):
    def __init__(self):
        self._tracer = None
        self.orig_execute = None
        self.orig_cursor_execute = None

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any) -> None:
        tracer_provider = kwargs.get('tracer_provider')
        capture_db_user = kwargs.get('capture_db_user', False)
        self._tracer = get_tracer(
            __name__,
            __version__,
            tracer_provider
        )

        self.orig_execute = Connection.execute
        self.orig_cursor_execute = Cursor.execute

        async def _execute(conn, query, *args, **kwargs):
            exception, result = None, None

            if not hasattr(conn, '_otel_db_attributes'):
                await _cache_db_attributes(conn, capture_db_user)

            operation_name = query.strip().split()[0]
            attributes = {
                **conn._otel_db_attributes,
                'db.query.text': query,
                'db.operation.name': operation_name
            }
            span_name = f"{operation_name} {conn._otel_db_attributes.get('db.namespace')}".strip()
            with self._tracer.start_as_current_span(span_name, kind=SpanKind.CLIENT, attributes=attributes) as span:
                return await self.orig_execute(conn, query, *args, **kwargs)

        async def _cursor_execute(conn, query, *args, **kwargs):
            exception, result = None, None

            if not hasattr(conn._conn, '_otel_db_attributes'):
                await _cache_db_attributes(conn._conn, capture_db_user)

            operation_name = query.strip().split()[0]
            attributes = {
                **conn._conn._otel_db_attributes,
                'db.query.text': query,
                'db.operation.name': operation_name
            }
            span_name = f"{operation_name} {conn._conn._otel_db_attributes.get('db.namespace')}".strip()
            with self._tracer.start_as_current_span(span_name, kind=SpanKind.CLIENT, attributes=attributes) as span:
                return await self.orig_cursor_execute(conn, query, *args, **kwargs)

        setattr(Connection, 'execute', _execute)
        setattr(Cursor, 'execute', _cursor_execute)

    def _uninstrument(self, **kwargs: Any):
        Connection.execute = self.orig_execute
        Cursor.execute = self.orig_cursor_execute
