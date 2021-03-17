from pyspark import SparkContext, SparkConf
from contextlib import ExitStack
import argparse
import os, sys

#Following lines are for assigning parent directory dynamically.

dir_path = os.path.dirname(os.path.realpath(__file__))

parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))

sys.path.insert(0, parent_dir_path)
from logzero import logger as logging
import timy
from distant_supervision.input_parser import InputParser

@timy.timer()
def main(sc):
    argp = argparse.ArgumentParser()
    argp.add_argument('--corpus', help='input corpus (*.raw)', required=True)
    argp.add_argument('--output-dir', default="output/", help='')
    argp.add_argument('--num-partitions', type=int, default=1, help='')
    argp.add_argument('--debug-save', help='for debugging purposes', action='store_true')
    args = argp.parse_args()

    metric_filename = os.path.join(args.output_dir, 'metric.txt')
    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    with ExitStack() as stack:
        metric_fptr = stack.enter_context(open(metric_filename, 'w'))

        data_raw = sc.textFile(args.corpus)

       # assert(args.corpus[-4:] == '.raw')
        print('Number of articles in raw: {}'.format(data_raw.count()), file=metric_fptr)
        metric_fptr.flush()

        input_parser = InputParser(
            num_partitions=args.num_partitions,
            debug_save=args.debug_save)

        # combine the data
        rollup_rdd = input_parser.tokenize_and_perform_rollup(data_raw)

        rollup_rdd.map(lambda x: x.jsonify()).saveAsTextFile(os.path.join(args.output_dir, 'rollup'))

        # do not do rollup_rdd.count(). It will recompute everything.

        logging.info('Output directory: {}'.format(args.output_dir))


if __name__ == '__main__':


    conf = SparkConf().setAppName("tokenize and NER inputs").setMaster("local")
    sc = SparkContext(conf=conf)
    sc.addFile('distant_supervision', recursive=True)

    main(sc)
