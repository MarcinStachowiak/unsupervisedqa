# --input-data-path-pattern=outputs/wiki_ner/*.txt --output-dir outputs/wiki_es_upload --processing-chunk-size 10000 --workers 10
import argparse
import os

from datashift import DataPipeline
from datashift.datapipeline import DefaultTextLineReader

from common.models import PhraseMode
from common.processors import TextLineToArticle
from ner_gatherer.entity_counter import CountEntities
from ner_gatherer.ner_gatherer import NerGatherer

if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('--input-data-path-pattern', help='', required=True)
    argp.add_argument('--output-dir', default="output/", help='', required=True)
    argp.add_argument('--phrase-mode', choices=[e.value for e in PhraseMode], default=PhraseMode.NER_ONLY.value,
                      help='Generate data using ner_only. Skip noun phrases')
    argp.add_argument('--entity-nchars-min-lim', default=3, type=int, help='')
    argp.add_argument('--num-articles-per-entity-min-lim', default=2, type=int, help='')
    argp.add_argument('--processing-chunk-size', default=10000, type=int, help='')
    argp.add_argument('--workers', default=1, type=int, help='')
    args = argp.parse_args()

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    output_metadata_file_path = '{}/entities_metadata.yaml'.format(args.output_dir)
    DataPipeline(
        reader=DefaultTextLineReader(
            input_data_path_pattern=args.input_data_path_pattern),
        processing_chunk_size=args.processing_chunk_size,
        num_workers=args.workers,
        output_metadata_file_path=output_metadata_file_path) \
        .process_task(TextLineToArticle()) \
        .process_task(NerGatherer(phrase_mode=PhraseMode[args.phrase_mode.upper()],
                                  entity_nchars_min_lim=args.entity_nchars_min_lim)) \
        .flatten() \
        .reduce_task(CountEntities(reduced_value_name='entities_count',
                                   num_articles_per_entity_min_lim=args.num_articles_per_entity_min_lim)) \
        .shift()
