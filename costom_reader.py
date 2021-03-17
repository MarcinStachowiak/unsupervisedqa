import argparse
import os

from datashift import AbstractProcessingTask
from datashift.datapipeline import AbstractReader, DataPipeline, DefaultCSVSaver
from elasticsearch import Elasticsearch
from datetime import datetime
from datetime import timedelta

from sshtunnel import SSHTunnelForwarder

from preprocess import TextCleaner, MinDescriptionLengthFilter, LanguageFilter, JointTitleAndDescription


class TDNetElasticsearchReader(AbstractReader):
    def __init__(self, es_host, es_index, start_date, end_date,delta, scroll_size='2m', sources='*'):
        self.es_host = es_host
        self.es_index = es_index
        self.scroll_size = scroll_size
        self.start_date = start_date
        self.end_date = end_date
        self.delta=delta
        self.sources = sources

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

    def setup(self):
        self.es = Elasticsearch([self.es_host])

    def scroll(self, es, index, body, scroll, size):
        page = es.search(index=index, body=body, scroll=scroll, size=size)
        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']
        print("%d documents found" % page['hits']['total']['value'])
        while len(hits):
            yield hits
            page = es.scroll(scroll_id=scroll_id, scroll=scroll)
            scroll_id = page['_scroll_id']
            hits = page['hits']['hits']

    def determine_chunked_execution_groups(self, pool, chunksize):
        return [(date_from, date_to, chunksize) for date_from, date_to in
                self._date_range(self.start_date, self.end_date, self.delta)]

    def next_data_chunk_gen(self, execution_groups):
        date_from = execution_groups[0]
        date_to = execution_groups[1]
        chunk_size = execution_groups[2]
        query_body = self._build_query(date_from, date_to, self.sources)
        return self.scroll(self.es, self.es_index, query_body, self.scroll_size, chunk_size)

    def _date_range(self, start, end, days_diff):
        start = datetime.strptime(start, "%Y-%m-%d").replace(hour=0, minute=0, second=0)
        end = datetime.strptime(end, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        delta = timedelta(days=days_diff)
        currentdate = start
        while currentdate + delta < end:
            todate = (currentdate + delta).replace(hour=23, minute=59, second=59)
            yield self._date_to_str(currentdate), self._date_to_str(todate)
            currentdate += delta
            currentdate.replace(hour=0, minute=0, second=0)
        yield self._date_to_str(start), self._date_to_str(end)

    def _date_to_str(self, date):
        return date.strftime("%Y-%m-%dT%H:%M:%SZ")


class ElasticsearchEntryToDict(AbstractProcessingTask):
    def process(self, sample):
        return {'id': sample['_id'],
                'title': sample['_source']['title'],
                'description': sample['_source']['description']['text']}


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('--input', help='input path for corpus data', required=True)
    argp.add_argument('--output-dir', default="output/", help='', required=True)
    argp.add_argument('--output-file-size', default=2000, type=int, help='', required=True)
    argp.add_argument('--processing-chunk-size', default=2000, type=int, help='')
    argp.add_argument('--workers', default=10, type=int, help='')
    argp.add_argument('--min_chars', default=150, type=int, help='')
    args = argp.parse_args()

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    server = SSHTunnelForwarder(
        '35.184.91.112',
        ssh_username="m_stachowiak_sigmoidal_io",
        ssh_pkey="id_rsa",
        remote_bind_address=('127.0.0.1', 9200),
        local_bind_address=('0.0.0.0', 9201)
    )

    server.start()
    DataPipeline(
        reader=TDNetElasticsearchReader(
            es_host='localhost:9201',
            es_index='tdnetindex_sigmoidal1',
            start_date='2021-01-04',
            end_date='2021-02-06',
            delta=1,
            sources=['title', 'description.text']),
        saver=DefaultCSVSaver(output_data_dir_path=args.output_dir, output_file_size=args.output_file_size,
                              output_file_name_prefix='cleaned'),
        processing_chunk_size=args.processing_chunk_size,
        num_workers=args.workers) \
        .process_task(ElasticsearchEntryToDict()) \
        .process_task(TextCleaner()) \
        .filter_task(MinDescriptionLengthFilter(min_char_length=args.min_chars)) \
        .filter_task(LanguageFilter('en')) \
        .process_task(JointTitleAndDescription()) \
        .shift()

    server.stop()
