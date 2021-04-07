# --input-data-path-pattern=outputs/wiki_queries/*.txt --output-dir outputs/wiki_questions --processing-chunk-size 10000 --workers 1 --es-host localhost --es-index wiki_ner_index --aux-qs=1 --aux-awc=1

import argparse
import os

from datashift import DataPipeline
from datashift.datapipeline import DefaultTextLineReader, DefaultTextLineSaver

from common.models import PhraseMode
from common.processors import TextLineToArticle
from entity_to_queries.processors import EntityToQueriesMapper, QueriesPerArticleObjToJson
from questions_generation.data_creator import SyntheticDataCreator, TextToQueriesPerArticleObj, FilterNone
from questions_generation.whxx_ngram_table import WhxxNgramTable

if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('--input-data-path-pattern', help='input path for corpus data', required=True)
    argp.add_argument('--processing-chunk-size', default=10000, type=int, help='')
    argp.add_argument('--workers', default=1, type=int, help='')
    argp.add_argument('--output-dir', default="output/", help='', required=True)
    argp.add_argument('--output-file-prefix', default="qa_", help='')
    argp.add_argument('--max-output-items-per-file', default=10000, type=int, help='')
    argp.add_argument('--es-hosts', help='', default='',required=True)
    argp.add_argument('--es-index-readonly', help='',required=True)
    argp.add_argument('--whxx-ngram-table', help='toml config file', default='resources/whxx_ngram_table.toml')
    argp.add_argument('--aux-qs', type=int, dest='nb_aux_qs_matches',
                      help='number of auxiliary entity matches with query sentence', default=0)
    argp.add_argument('--aux-awc', type=int, dest='nb_aux_awc_matches',
                      help='number of additional aux matches with anywhere in context (in additional to aux-qs)',
                      default=0)

    args = argp.parse_args()

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    with open(args.whxx_ngram_table) as fptr:
        whxx_ngram_table = WhxxNgramTable.import_from_toml(fptr)

    DataPipeline(
        reader=DefaultTextLineReader(input_data_path_pattern=args.input_data_path_pattern),
        processing_chunk_size=args.processing_chunk_size,
        num_workers=args.workers,
        saver=DefaultTextLineSaver(
            output_data_dir_path=args.output_dir,
            output_file_name_prefix=args.output_file_prefix,
            output_file_size=args.max_output_items_per_file)) \
        .process_task(TextToQueriesPerArticleObj()) \
        .process_task(SyntheticDataCreator(es_hosts=args.es_hosts,
                                           es_index_name=args.es_index_readonly,
                                           whxx_ngram_table=whxx_ngram_table,
                                           nb_aux_qs_matches=args.nb_aux_qs_matches,
                                           nb_aux_awc_matches=args.nb_aux_awc_matches,
                                           phrase_mode=PhraseMode.NER_ONLY)) \
        .filter_task(FilterNone()) \
        .shift()
