import argparse
import os

from datashift import DataPipeline
from datashift.datapipeline import DefaultTextLineReader

from distant_supervision.data_models import PhraseMode
from distant_supervision.ner_entity_gatherer import TextLineToArticle, CountEntities, NerEntityGatherer

if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('--corpus', help='input path for corpus data', required=True)
    argp.add_argument('--output-dir', default="output/", help='', required=True)
    argp.add_argument('--phrase-mode', choices=[e.value for e in PhraseMode], default=PhraseMode.NER_ONLY.value,
                      help='Generate data using ner_only. Skip noun phrases')
    argp.add_argument('--processing-chunk-size', default=10000, type=int, help='')
    argp.add_argument('--workers', default=10, type=int, help='')
    args = argp.parse_args()

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    output_metadata_file_path = '{}/entities_metadata.yaml'.format(args.output_dir)
    DataPipeline(
        reader=DefaultTextLineReader(
            input_data_path_pattern=args.corpus),
        processing_chunk_size=args.processing_chunk_size,
        num_workers=args.workers,
        output_metadata_file_path=output_metadata_file_path) \
        .process_task(TextLineToArticle()) \
        .process_task(NerEntityGatherer(phrase_mode=PhraseMode[args.phrase_mode.upper()])) \
        .flatten() \
        .reduce_task(CountEntities('entities_count')) \
        .shift()
