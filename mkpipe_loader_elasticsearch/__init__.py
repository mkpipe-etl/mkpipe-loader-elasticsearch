import gc
from datetime import datetime

from mkpipe.spark.base import BaseLoader
from mkpipe.spark.columns import add_etl_columns
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.utils import get_logger

logger = get_logger(__name__)


class ElasticsearchLoader(BaseLoader, variant='elasticsearch'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.host = connection.host or 'localhost'
        self.port = connection.port or 9200
        self.username = connection.user
        self.password = connection.password
        self.scheme = connection.extra.get('scheme', 'http')
        self.api_key = connection.api_key

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        write_mode = data.write_mode
        df = data.df

        if df is None:
            logger.info({'table': target_name, 'status': 'skipped', 'reason': 'no data'})
            return

        df = add_etl_columns(df, datetime.now(), dedup_columns=table.dedup_columns)

        logger.info({
            'table': target_name,
            'status': 'loading',
            'write_mode': write_mode,
        })

        from elasticsearch import Elasticsearch, helpers

        es_kwargs = {
            'hosts': [f'{self.scheme}://{self.host}:{self.port}'],
            'verify_certs': self.connection.extra.get('verify_certs', False),
        }
        if self.api_key:
            es_kwargs['api_key'] = self.api_key
        elif self.username and self.password:
            es_kwargs['basic_auth'] = (self.username, self.password)

        es = Elasticsearch(**es_kwargs)

        if write_mode == 'overwrite':
            if es.indices.exists(index=target_name):
                es.indices.delete(index=target_name)
                logger.info({'table': target_name, 'status': 'index_deleted'})

        rows = [row.asDict(recursive=True) for row in df.collect()]
        batchsize = table.batchsize or 10000

        for i in range(0, len(rows), batchsize):
            batch = rows[i:i + batchsize]
            actions = []
            for row in batch:
                doc_id = row.pop('_id', None)
                action = {
                    '_index': target_name,
                    '_source': row,
                }
                if doc_id:
                    action['_id'] = doc_id
                actions.append(action)
            helpers.bulk(es, actions, raise_on_error=True)

        df.unpersist()
        gc.collect()

        logger.info({
            'table': target_name,
            'status': 'loaded',
            'rows': len(rows),
        })
