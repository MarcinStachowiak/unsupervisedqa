# --input-data-path-pattern=outputs/wiki_ner/*.txt --output-dir outputs/wiki_es_upload --es-host localhost --es-index wiki_ner_index --processing-chunk-size 1000 --no-items-per-request 100 --workers 1 --ssh-host 107.22.222.219 --ssh-username ubuntu --ssh-pkey tdnet-elasticsearch-server.pem --ssh-remote-bind-port 9200 --ssh-local-bind-port 9201
import argparse
import logging
import os
import sys

from datashift import DataPipeline
from datashift.datapipeline import DefaultTextLineReader
from sshtunnel import SSHTunnelForwarder

from common.processors import TextLineToArticle
from common.reducer import DefaultCounter
from ner_es_uploader.es_setup import setup_es
from ner_es_uploader.es_uploader import ESIndexWriter

dir_path = os.path.dirname(os.path.realpath(__file__))
parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
sys.path.insert(0, parent_dir_path)


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('--input-data-path-pattern', help='', required=True)
    argp.add_argument('--output-dir', default="output/", help='')
    argp.add_argument('--es-hosts', help='', default='')
    argp.add_argument('--es-index', help='', required=True)
    argp.add_argument('--processing-chunk-size', default=10000, type=int, help='')
    argp.add_argument('--workers', default=10, type=int, help='')
    argp.add_argument('--no-items-per-request', default=100, type=int, help='')
    argp.add_argument('--ssh-host', type=str, help='', required=False)
    argp.add_argument('--ssh-username', type=str, help='', required=False)
    argp.add_argument('--ssh-pkey', type=str, help='', required=False)
    argp.add_argument('--ssh-remote-bind-port', type=int, help='', required=False)
    argp.add_argument('--ssh-local-bind-port', type=int, help='', required=False)
    args = argp.parse_args()

    logging.getLogger('elasticsearch').setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    if args.ssh_host is not None:
        server = SSHTunnelForwarder(
            args.ssh_host,
            ssh_username=args.ssh_username,
            ssh_pkey=args.ssh_pkey,
            remote_bind_address=('127.0.0.1', args.ssh_remote_bind_port),
            local_bind_address=('0.0.0.0', args.ssh_local_bind_port)
        )
        server.start()

    setup_es(es_host=args.es_hosts, es_index=args.es_index)

    DataPipeline(
        reader=DefaultTextLineReader(input_data_path_pattern=args.input_data_path_pattern),
        output_metadata_file_path='{}/elasticksearch_upload_metadata.yaml'.format(args.output_dir),
        processing_chunk_size=args.processing_chunk_size,
        num_workers=args.workers) \
        .process_task(TextLineToArticle()) \
        .process_task(ESIndexWriter(no_items_per_request=args.no_items_per_request,
                                    es_hosts=args.es_hosts,
                                    es_index=args.es_index,
                                    es_doc_type='doc')) \
        .flatten() \
        .reduce_task(DefaultCounter('no_saved_items_to_es')) \
        .shift()

    if args.ssh_host is not None:
        server.stop()

    logging.info('Loaded elements to the Elasticksearch')
