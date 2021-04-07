# --input-data-path-pattern=outputs/wiki_ner/*.txt --processing-chunk-size 10000 --workers 1 --reduce-file-path outputs/wiki_es_upload/entities.csv
import argparse
import os
import csv
from pathlib import Path

from datashift import DataPipeline
from datashift.datapipeline import DefaultTextLineReader

from common.models import PhraseMode
from common.processors import TextLineToArticle
from ner_gatherer.entity_counter import CountEntities
from ner_gatherer.ner_gatherer import NerGatherer



if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('--input-data-path-pattern', help='', required=True)
    argp.add_argument('--phrase-mode', choices=[e.value for e in PhraseMode], default=PhraseMode.NER_ONLY.value,
                      help='Generate data using ner_only. Skip noun phrases')
    argp.add_argument('--entity-nchars-min-lim', default=3, type=int, help='')
    argp.add_argument('--num-articles-per-entity-min-lim', default=2, type=int, help='')
    argp.add_argument('--processing-chunk-size', default=10000, type=int, help='')
    argp.add_argument('--workers', default=1, type=int, help='')
    argp.add_argument('--reduce-file-path',  type=str, help='',required=True)
    args = argp.parse_args()

    def save_custom_reduce_callback(dict_to_save):
        Path(args.reduce_file_path).parent.mkdir(parents=True, exist_ok=True)
        f = open(args.reduce_file_path, "w")
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["text", "type", "count"])
        for k, v in dict_to_save['entities_count'].items():
            writer.writerow([k[0], k[1], v])
        f.close()

    DataPipeline(
        reader=DefaultTextLineReader(
            input_data_path_pattern=args.input_data_path_pattern),
        processing_chunk_size=args.processing_chunk_size,
        num_workers=args.workers,
        custom_reduce_save_callback=save_custom_reduce_callback) \
        .process_task(TextLineToArticle()) \
        .process_task(NerGatherer(phrase_mode=PhraseMode[args.phrase_mode.upper()],
                                  entity_nchars_min_lim=args.entity_nchars_min_lim)) \
        .flatten() \
        .reduce_task(CountEntities(reduced_value_name='entities_count',
                                   num_articles_per_entity_min_lim=args.num_articles_per_entity_min_lim)) \
        .shift()
