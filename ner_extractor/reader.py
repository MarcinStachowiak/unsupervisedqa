from datetime import timedelta, datetime
from logging import getLogger
import os
from datashift.datapipeline import AbstractReader, AbstractDataBucket
from elasticsearch import Elasticsearch
import time


class TDNetElasticsearchDataBucket(AbstractDataBucket):
    def __init__(self, data_buckets, es_host, es_index,scroll_size,sources):
        self.scroll_id = None
        self.es_index = es_index
        self.es_host = es_host
        self.scroll_size=scroll_size
        self.logger = getLogger('datashift')
        assert len(data_buckets) == 3
        date_from = data_buckets[0]
        date_to = data_buckets[1]
        self.body = self._build_query(date_from, date_to, sources)
        self.chunk_size = data_buckets[2]
        self.start_time = None
        super().__init__(data_buckets=data_buckets)

    def setup(self):
        self.es = Elasticsearch([self.es_host])

    def _build_query(self, date_from, date_to, sources):
        return {
            "_source": sources,
            "query": {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": [
                                    {
                                        "term": {"language": "English"}
                                    },
                                    {
                                        "bool": {
                                            "must_not": [
                                                {
                                                    "exists": {
                                                        "field": "language"
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        }, {
                            "range": {
                                "harvest_date": {
                                    "gte": "{}".format(date_from),
                                    "lte": "{}".format(date_to),
                                }
                            }
                        },
                        {
                            "exists": {"field": "description.text"}
                        },
                        {
                            "exists": {"field": "title"}
                        }
                    ]
                }
            }
        }

    def next_data_chunk(self):
        if self.scroll_id is None:
            page = self.es.search(index=self.es_index, body=self.body, scroll=self.scroll_size, size=self.chunk_size)
            self.logger.info('Process {} - {} documents found'.format(os.getpid(), page['hits']['total']['value']))
        else:
            page = self.es.scroll(scroll_id=self.scroll_id, scroll=self.scroll_size)
        self.scroll_id = page['_scroll_id']
        hits = page['hits']['hits']
        if self.start_time is not None:
            self.logger.info(
                'Process {} - Elasticsearch scroll time needed: {}s'.format(os.getpid(), time.time() - self.start_time))
        self.start_time = time.time()
        return hits


class TDNetElasticsearchReader(AbstractReader):
    def __init__(self, es_host, es_index, start_date, end_date, delta, scroll_size='1m', sources='*'):
        self.es_host = es_host
        self.es_index = es_index
        self.scroll_size = scroll_size
        self.start_date = start_date
        self.end_date = end_date
        self.delta = delta
        self.sources = sources

    def build_data_buckets(self, _, chunksize):
        return [TDNetElasticsearchDataBucket((date_from, date_to, chunksize),self.es_host,self.es_index,self.scroll_size,self.sources) for date_from, date_to in
                self._date_range(self.start_date, self.end_date, self.delta)]

    def _date_range(self, start, end, days_diff):
        start = datetime.strptime(start, "%Y-%m-%d").replace(hour=0, minute=0, second=0)
        end = datetime.strptime(end, "%Y-%m-%d").replace(hour=0, minute=0, second=0)
        delta = timedelta(days=days_diff)
        currentdate = start
        while currentdate + delta < end:
            todate = (currentdate + delta).replace(hour=0, minute=0, second=0)
            yield self._date_to_str(currentdate), self._date_to_str(todate)
            currentdate += delta
            currentdate.replace(hour=0, minute=0, second=0)
        yield self._date_to_str(start), self._date_to_str(end)

    def _date_to_str(self, date):
        return date.strftime("%Y-%m-%dT%H:%M:%SZ")
