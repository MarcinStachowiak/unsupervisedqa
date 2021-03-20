import argparse
import os

from datashift import DataPipeline, DefaultTextLineSaver, DefaultCSVReader
from sshtunnel import SSHTunnelForwarder

from ner_extractor.filters import MinDescriptionLengthFilter, LanguageFilter
from ner_extractor.processors import ElasticsearchEntryToArticle, ArticleTextCleaner, SentenceSplitterAndNerExtractor, \
    ArticleToJsonProcessTask
from ner_extractor.reader import TDNetElasticsearchReader

if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('--input-data-path-pattern', help='', required=True)
    argp.add_argument('--output-dir', help='', required=True)
    argp.add_argument('--output-file-size', default=50000, type=int, help='')
    argp.add_argument('--processing-chunk-size', default=10000, type=int, help='')
    argp.add_argument('--output-file-prefix', type=str, help='', required=True)
    argp.add_argument('--workers', default=1, type=int, help='')
    argp.add_argument('--min-text-length-chars', default=150, type=int, help='')
    args = argp.parse_args()

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    DataPipeline(
        reader=DefaultCSVReader(input_data_path_pattern=args.input_data_path_pattern,
                                input_columns=["id", "title", "description"]),
        saver=DefaultTextLineSaver(output_data_dir_path=args.output_dir,
                                   output_file_size=args.output_file_size,
                                   output_file_name_prefix=args.output_file_prefix),
        processing_chunk_size=args.processing_chunk_size,
        num_workers=args.workers) \
        .process_task(ElasticsearchEntryToArticle()) \
        .process_task(ArticleTextCleaner()) \
        .filter_task(MinDescriptionLengthFilter(min_char_length=args.min_text_length_chars)) \
        .filter_task(LanguageFilter('en')) \
        .process_task(SentenceSplitterAndNerExtractor(ulim_char_per_sentence=500)) \
        .process_task(ArticleToJsonProcessTask()) \
        .shift()

# --input-data-path-pattern=outputs/raw/*/*.csv --output-dir outputs/medical_ner --output-file-prefix ner_ --processing-chunk-size 10000 --output-file-size 50000 --workers 1 --min-text-length-chars 150
