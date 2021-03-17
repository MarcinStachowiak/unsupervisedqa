import argparse
import os

from datashift import DataPipeline, DefaultTextLineSaver
from sshtunnel import SSHTunnelForwarder

from ner_extractor.filters import MinDescriptionLengthFilter, LanguageFilter
from ner_extractor.processors import ElasticsearchEntryToArticle, ArticleTextCleaner, SentenceSplitterAndNerExtractor, \
    ArticleToJsonProcessTask
from ner_extractor.reader import TDNetElasticsearchReader

if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('--output-dir', default="output/", help='', required=True)
    argp.add_argument('--output-file-size', default=50000, type=int, help='')
    argp.add_argument('--processing-chunk-size', default=100, type=int, help='')
    argp.add_argument('--output-file-prefix', type=str, help='',required=True)
    argp.add_argument('--workers', default=1, type=int, help='')
    argp.add_argument('--min-text-length-chars', default=150, type=int, help='')
    argp.add_argument('--es-host', type=str, help='',required=True)
    argp.add_argument('--es-index', type=str, help='',required=True)
    argp.add_argument('--start-date', type=str, help='', required=True)
    argp.add_argument('--end-date', type=str, help='', required=True)
    argp.add_argument('--delta', default=1, type=int, help='', required=True)
    argp.add_argument('--scroll-size', type=str, default='2m', help='', required=False)
    argp.add_argument('--ssh-host', type=str, help='', required=False)
    argp.add_argument('--ssh-username', type=str, help='', required=False)
    argp.add_argument('--ssh-pkey', type=str, help='', required=False)
    argp.add_argument('--ssh-remote-bind-port', type=int, help='', required=False)
    argp.add_argument('--ssh-local-bind-port', type=int, help='', required=False)
    args = argp.parse_args()

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

    DataPipeline(
        reader=TDNetElasticsearchReader(
            es_host=args.es_host,
            es_index=args.es_index,
            start_date=args.start_date,
            end_date=args.end_date,
            delta=args.delta,
            scroll_size=args.scroll_size,
            sources=['title', 'description.text']),
        saver=DefaultTextLineSaver(output_data_dir_path=args.output_dir,output_file_size=args.output_file_size,output_file_name_prefix=args.output_file_prefix),
        processing_chunk_size=args.processing_chunk_size,
        num_workers=args.workers) \
        .process_task(ElasticsearchEntryToArticle()) \
        .process_task(ArticleTextCleaner()) \
        .filter_task(MinDescriptionLengthFilter(min_char_length=args.min_text_length_chars)) \
        .filter_task(LanguageFilter('en')) \
        .process_task(SentenceSplitterAndNerExtractor(ulim_char_per_sentence=500)) \
        .process_task(ArticleToJsonProcessTask()) \
        .shift()

    if args.ssh_host is not None:
        server.stop()
# --output-dir /unsupervisedqa --output-file-prefix ner_ --processing-chunk-size 100 --output-file-size 2000 --workers 1 --min-text-length-chars 150 --es-host localhost:9200 --es-index tdnetindex_sigmoidal1 --start-date 2021-01-04 --end-date 2021-02-06 --delta 1
# --ssh-host 35.184.91.112 --ssh-username m_stachowiak_sigmoidal_io --ssh-pkey  id_rsa  --ssh-remote-bind-port  9200  --ssh-local-bind-port  9201