from datashift import DataPipeline
from datashift.datapipeline import DefaultTextLineReader, DefaultListReader, DefaultTextLineSaver
from pyspark import SparkContext, SparkConf
from contextlib import ExitStack
import argparse
import subprocess
import json
import sys
import os
from logzero import logger as logging
import timy
import yaml
from distant_supervision import utils
from distant_supervision.entity_to_queries_mapper import Entity2QpaProcessingTask, Entity2QpaGroupByEntity
from distant_supervision.ner_entity_gatherer import TextLineToArticle, UniqueEntityExtractor, CountEntities, _clean_ners
from distant_supervision.whxx_ngram_table import WhxxNgramTable
from distant_supervision.data_models import Article, PhraseObj, PhraseMode
from distant_supervision.synthetic_data_creator import SyntheticDataCreator, QUESTION_STYLES_FOR_JSONLINES, \
    ObtainRetrievedSentencesProcessingTask


def _create_jsonl_training_files(output_dir, qstyle):
    dest_200k_jsonl_filepath = os.path.join(output_dir, '{}_200k.jsonl'.format(qstyle.value))
    cmd_lst = []

    cmd_lst.append('cat {}/part-* | shuf -n 200000 > {}'.format(
        os.path.join(output_dir, qstyle.value),
        dest_200k_jsonl_filepath))

    for kilo_head_count in [50, 100]:
        dest_jsonl_filepath = os.path.join(output_dir, '{}_{}k.jsonl'.format(qstyle.value, kilo_head_count))
        cmd_lst.append('head -n {} {} > {}'.format(
            kilo_head_count * 1000,
            dest_200k_jsonl_filepath,
            dest_jsonl_filepath))

    for cmd in cmd_lst:
        logging.info('# {}'.format(cmd))
        subprocess.run(cmd, shell=True)


@timy.timer()
def main():
    default_config = utils.read_default_config_toml()

    argp = argparse.ArgumentParser()
    argp.add_argument('--corpus', help='input path for corpus data', required=True)
    argp.add_argument('--output-dir', default="output/", help='', required=True)
    argp.add_argument('--ulim-count', type=int, default=None,
                      help='approximate output count. Does not change NER ulim')
    argp.add_argument('--es-hosts', help='', default='')
    argp.add_argument('--es-index-readonly', help='', default=default_config['es_index_readonly'])
    argp.add_argument('--whxx-ngram-table', help='toml config file', default='resources/whxx_ngram_table.toml')
    argp.add_argument('--num-partitions', type=int, default=10, help='')
    argp.add_argument('--debug-save', help='for debugging purposes', action='store_true')

    argp.add_argument('--ulim-ner', default=None, type=int, help='upper limit of NER')
    argp.add_argument('--ner',
                      help='NER entity2articles folder. If none is given, NER set is computed from corpus.')

    argp.add_argument('--phrase-mode', choices=[e.value for e in PhraseMode], default=PhraseMode.NER_ONLY.value,
                      help='Generate data using ner_only. Skip noun phrases')

    argp.add_argument('--aux-qs', type=int, dest='nb_aux_qs_matches',
                      help='number of auxiliary entity matches with query sentence', default=0)
    argp.add_argument('--aux-awc', type=int, dest='nb_aux_awc_matches',
                      help='number of additional aux matches with anywhere in context (in additional to aux-qs)',
                      default=0)
    args = argp.parse_args()

    assert args.es_hosts and args.es_index_readonly

    logging.info('es index: {}'.format(args.es_index_readonly))

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    with open(args.whxx_ngram_table) as fptr:
        whxx_ngram_table = WhxxNgramTable.import_from_toml(fptr)

    output_metadata_file_path='{}/entities_metadata.yaml'.format(args.output_dir)

    with open(output_metadata_file_path) as file:
        entities_count = yaml.load(file, Loader=yaml.FullLoader)['entities_count']
    ner_set=set()
    for k in entities_count.keys():
        ner_set.add(eval(k)) #TODO maybe clean strings for '?
    ner_set = _clean_ners(ner_set)

    if args.ulim_ner:
        ner_lst = list(ner_set)
        rng = utils.RandomNumberGenerator()
        rng.vanilla.shuffle(ner_lst)
        ner_set = set(ner_lst[:args.ulim_ner])

    entity2qpa_file='{}/entity2qpa.yaml'.format(args.output_dir)
    entity2qpa=DataPipeline(
        reader=DefaultTextLineReader(input_data_path_pattern=args.corpus), processing_chunk_size=10, num_workers=1,
        output_metadata_file_path=entity2qpa_file, saver=None) \
        .process_task(TextLineToArticle()) \
        .process_task(Entity2QpaProcessingTask(ner_set=ner_set,phrase_mode=PhraseMode.NER_ONLY)) \
        .flatten() \
        .reduce_task(Entity2QpaGroupByEntity('entity2qpa')) \
        .shift()

    DataPipeline(
        reader=DefaultListReader([(k,v) for k,v in entity2qpa[0][1].items()]), processing_chunk_size=10, num_workers=1,
        saver=DefaultTextLineSaver(output_data_dir_path=args.output_dir, output_file_name_prefix='qa_',
                                   output_file_size=1000)) \
        .process_task(ObtainRetrievedSentencesProcessingTask(chunk_size=20,es_hosts=args.es_hosts,es_index_name=args.es_index_readonly,whxx_ngram_table=whxx_ngram_table,
                                                             nb_aux_qs_matches=args.nb_aux_qs_matches,nb_aux_awc_matches=args.nb_aux_awc_matches,phrase_mode=PhraseMode.NER_ONLY)) \
        .flatten() \
        .shift()



    sys.exit()


    #
    # with ExitStack() as stack:
    #     metric_fptr = stack.enter_context(open(os.path.join(args.output_dir, 'metric.txt'), 'w'))
    #     print('CMD: {}'.format(' '.join(sys.argv)), file=metric_fptr)
    #
    #     article_rdd = sc.textFile(args.corpus, minPartitions=args.num_partitions).map(Article.deserialize_json)
    #
    #     job = SyntheticDataCreator(
    #         args.output_dir,
    #         ulim_count=args.ulim_count,
    #         es_hosts=args.es_hosts,
    #         es_index_name=args.es_index_readonly,
    #         whxx_ngram_table=whxx_ngram_table,
    #         nb_ner_ulim=args.ulim_ner,
    #         num_partitions=args.num_partitions,
    #         nb_aux_qs_matches=args.nb_aux_qs_matches,
    #         nb_aux_awc_matches=args.nb_aux_awc_matches,
    #         phrase_mode=PhraseMode(args.phrase_mode),
    #         debug_save=args.debug_save)
    #     job.run_job(sc, article_rdd, ner_rdd, metric_fptr)
    #
    #     logging.info('Output directory: {}'.format(args.output_dir))
    #
    #
    #
    #
    # for qstyle in QUESTION_STYLES_FOR_JSONLINES:
    #     _create_jsonl_training_files(args.output_dir, qstyle)


if __name__ == '__main__':
    main()