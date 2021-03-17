import csv
import os

from elasticsearch import Elasticsearch
from sshtunnel import SSHTunnelForwarder
import time

server = SSHTunnelForwarder(
    '35.184.91.112',
    ssh_username="m_stachowiak_sigmoidal_io",
    ssh_pkey="id_rsa",
    remote_bind_address=('127.0.0.1', 9200),
    local_bind_address=('0.0.0.0', 9201)
)

server.start()

query_body = {
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
                            "gte": "2021-01-04T00:00:00.000+00:00",
                            "lte": "2021-01-06T23:59:59.999+00:00",
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

fields = ['id', 'publish_date', 'harvest_date', 'document_types', 'title', 'description']
output_dir = 'outputs/es_raw_data'
file_iter = 0
size = 500
OUT_FILE_SIZE = 30000
buffer = []

if not os.path.exists(output_dir):
    os.mkdir(output_dir)


def retrirve_document_types(item):
    if 'document_types' in item['_source']:
        return item['_source']['document_types']
    else:
        return ''

def retrirve_publish_date(item):
    if 'publish_date' in item['_source']:
        return item['_source']['publish_date']['date']
    else:
        return ''


def add_to_buffer(data, buffer):
    for item in data:
        buffer.append([item['_id'], retrirve_publish_date(item), item['_source']['harvest_date'],
                       retrirve_document_types(item), item['_source']['title'], item['_source']['description']['text']])


def collect_and_save(data, buffer,file_iter):
    if len(buffer) + len(data) < OUT_FILE_SIZE:
        add_to_buffer(data, buffer)
    else:
        remaining = OUT_FILE_SIZE - len(buffer)
        add_to_buffer(data[:remaining], buffer)
        save_buffer(buffer,file_iter)
        add_to_buffer(data[remaining:], buffer)


def save_buffer(buffer,file_iter):
    filename = '{}/es_raw_data_{}.csv'.format(output_dir, file_iter)
    with open(filename, 'w') as f:
        write = csv.writer(f, quoting=csv.QUOTE_ALL)
        write.writerow(fields)
        write.writerows(buffer)
    print("Saved {} items to the {} file".format(len(buffer), filename))
    buffer.clear()


def scroll(es, index, body, scroll, size, **kw):
    page = es.search(index=index, body=body, scroll=scroll, size=size, **kw)
    scroll_id = page['_scroll_id']
    hits = page['hits']['hits']
    print("%d documents found" % page['hits']['total']['value'])
    while len(hits):
        yield hits
        page = es.scroll(scroll_id=scroll_id, scroll=scroll)
        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']


es = Elasticsearch(["localhost:9201"])
st = time.time()
for chunk in scroll(es=es, index="tdnetindex_sigmoidal1", body=query_body, size=size, scroll='2m'):
    collect_and_save(chunk, buffer,file_iter)
    file_iter += 1

save_buffer(buffer,file_iter)

server.stop()
