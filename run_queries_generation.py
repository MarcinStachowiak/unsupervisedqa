# --input-data-path-pattern=outputs/wiki_ner/*11*.txt --output-dir outputs/wiki_queries --processing-chunk-size 10000 --workers 1 --ner-statistics-path outputs/wiki_es_upload/entities.csv
import argparse
import os

from datashift import DataPipeline
from datashift.datapipeline import DefaultTextLineReader, DefaultTextLineSaver

from common.models import PhraseMode
from common.processors import TextLineToArticle
from entity_to_queries.processors import EntityToQueriesMapper, QueriesPerArticleObjToJson

if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('--input-data-path-pattern', help='input path for corpus data', required=True)
    argp.add_argument('--processing-chunk-size', default=10000, type=int, help='')
    argp.add_argument('--workers', default=1, type=int, help='')
    argp.add_argument('--output-dir', default="output/", help='', required=True)
    argp.add_argument('--output-file-prefix', default="queries_", help='')
    argp.add_argument('--max-output-items-per-file', default=10000, type=int, help='')
    argp.add_argument('--ulim-ner', type=int, default=None, help='')
    argp.add_argument('--ner-statistics-path', type=str, default=None, help='', required=True)
    argp.add_argument('--phrase-mode', choices=[e.value for e in PhraseMode], default=PhraseMode.NER_ONLY.value,
                      help='Generate data using ner_only. Skip noun phrases')
    argp.add_argument('--answer-max-num-chars', type=int, default=30, help='')
    argp.add_argument('--num-entities-per-article-to-consider', type=int, default=30, help='')
    argp.add_argument('--max-words-in-query-sentence', type=int, default=100, help='')
    argp.add_argument('--max-num-of-sentences', type=int, default=8, help='')
    argp.add_argument('--num-entities-per-article-to-keep', type=int, default=5, help='')
    argp.add_argument('--max-words-in-context', type=int, default=400, help='')

    args = argp.parse_args()

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    # if args.ulim_ner:
    #     ner_lst = list(ner_set)
    #     rng = utils.RandomNumberGenerator()
    #     rng.vanilla.shuffle(ner_lst)
    #     ner_set = set(ner_lst[:args.ulim_ner])

    DataPipeline(
        reader=DefaultTextLineReader(input_data_path_pattern=args.input_data_path_pattern),
        processing_chunk_size=args.processing_chunk_size,
        num_workers=args.workers,
        saver=DefaultTextLineSaver(
            output_data_dir_path=args.output_dir,
            output_file_name_prefix=args.output_file_prefix,
            output_file_size=args.max_output_items_per_file)) \
        .process_task(TextLineToArticle()) \
        .process_task(EntityToQueriesMapper(ner_statistics_path=args.ner_statistics_path,
                                            ulim_ner=args.ulim_ner,
                                            phrase_mode=PhraseMode[args.phrase_mode.upper()],
                                            answer_max_num_chars=args.answer_max_num_chars,
                                            num_entities_per_article_to_consider=args.num_entities_per_article_to_consider,
                                            max_words_in_query_sentence=args.max_words_in_query_sentence,
                                            max_num_of_sentences=args.max_num_of_sentences,
                                            num_entities_per_article_to_keep=args.num_entities_per_article_to_keep,
                                            max_words_in_context=args.max_words_in_context)) \
        .flatten() \
        .process_task(QueriesPerArticleObjToJson()) \
        .shift()
