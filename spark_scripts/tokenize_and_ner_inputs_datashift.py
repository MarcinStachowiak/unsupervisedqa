import argparse
import argparse
import os
import sys
from datashift import DataPipeline, DefaultTextLineReader, AbstractProcessingTask, NotNoneFilterTask
# Following lines are for assigning parent directory dynamically.
from datashift.datapipeline import DefaultTextLineSaver

dir_path = os.path.dirname(os.path.realpath(__file__))
parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
sys.path.insert(0, parent_dir_path)
from logzero import logger as logging
from distant_supervision.input_parser import InputParser

class ToJsonProcessTask(AbstractProcessingTask):
    def process(self, sample):
        return sample.jsonify()

if __name__ == '__main__':
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    argp = argparse.ArgumentParser()
    argp.add_argument('--corpus', help='input corpus (*.raw)', required=True)
    argp.add_argument('--output-dir', default="output/", help='')
    argp.add_argument('--output-file-prefix', default="ner_", help='')
    argp.add_argument('--max-output-items-per-file', default=10000, type=int, help='')
    argp.add_argument('--processing-chunk-size', default=10000, type=int, help='')
    argp.add_argument('--workers', default=1, type=int, help='')
    args = argp.parse_args()
    DataPipeline(
        reader=DefaultTextLineReader(
            input_data_path_pattern=args.corpus),
        saver=DefaultTextLineSaver(
            output_data_dir_path=args.output_dir,
            output_file_name_prefix=args.output_file_prefix,
            output_file_size=args.max_output_items_per_file),
        processing_chunk_size=args.processing_chunk_size,
        num_workers=args.workers) \
        .process_task(InputParser()) \
        .filter_task(NotNoneFilterTask()) \
        .process_task(ToJsonProcessTask()) \
        .shift()
    logging.info('Output directory: {}'.format(args.output_dir))
