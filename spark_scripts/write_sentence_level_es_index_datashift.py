import argparse
import os
import logging
import sys

from datashift import DataPipeline, AbstractReduceTask
from datashift.datapipeline import DefaultTextLineReader
dir_path = os.path.dirname(os.path.realpath(__file__))
parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
sys.path.insert(0, parent_dir_path)


from distant_supervision.ds_es_client import ESIndexWriter
from distant_supervision.ner_entity_gatherer import TextLineToArticle


class DefaultCounter(AbstractReduceTask):
    def reduce_locally(self, samples):
        return len(samples)

    def reduce_globally(self, local_reductions):
        return sum(local_reductions)


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('--corpus', help='input path, e.g. foobar/rollup/', required=True)
    argp.add_argument('--output-dir', default="output/", help='')
    argp.add_argument('--es-hosts', help='', default='')
    argp.add_argument('--es-index', help='', required=True)
    argp.add_argument('--processing-chunk-size', default=10000, type=int, help='')
    argp.add_argument('--workers', default=10, type=int, help='')
    argp.add_argument('--no-items-per-request', default=100, type=int, help='')
    args = argp.parse_args()

    logging.getLogger('elasticsearch').setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    esi_intex_writer = ESIndexWriter(no_items_per_request=args.no_items_per_request,
                                     es_hosts=args.es_hosts,
                                     es_index=args.es_index,
                                     es_doc_type='doc')

    esi_intex_writer.create_es_index(args.es_hosts,args.es_index)

    metadata = DataPipeline(
        reader=DefaultTextLineReader(input_data_path_pattern=args.corpus),
        output_metadata_file_path='{}/elasticksearch_upload_metadata.yaml'.format(args.output_dir),
        processing_chunk_size=args.processing_chunk_size,
        num_workers=args.workers) \
        .process_task(TextLineToArticle()) \
        .process_task(esi_intex_writer) \
        .flatten()\
        .reduce_task(DefaultCounter('no_saved_items_to_es')) \
        .shift()

    logging.info('Loaded {} elements to the Elasticksearch'.format(metadata[1]))
