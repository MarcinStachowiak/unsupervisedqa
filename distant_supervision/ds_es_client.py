import os
from logging import getLogger

from datashift import AbstractProcessingTask
from elasticsearch import helpers
import json

from elasticsearch6 import Elasticsearch

from .utils import ElasticsearchConfig, ElasticsearchMagic


class ESIndexWriter(AbstractProcessingTask):
    def __init__(self, es_hosts,es_index,es_doc_type,no_items_per_request):
        self.es_hosts=es_hosts
        self.es_index=es_index
        self.es_doc_type=es_doc_type
        self.no_items_per_request=no_items_per_request

    def setup(self):
        self.es=Elasticsearch(hosts=[self.es_hosts])
        self.logger=getLogger('datashift')
        self.pid=os.getpid()

    def process(self, data):
        if type(data) is not list:
            data=[data]
        data=[self._filter_sents(article) for article in data]
        requests_generator=self._generate_requests_for_articles(data)
        self.logger.info("Uploading {} elements to Elasticksearch for process {}".format(len(data),self.pid))
        helpers.bulk(self.es, requests_generator, request_timeout=120)
        return data

    def _generate_requests_for_articles(self,article_lst):
            for article in article_lst:
                for sent_obj in article.sents:
                    text_body = sent_obj.text
                    yield {
                        '_op_type': 'create',  # `create` will fail on duplicate _id
                        "_index": self.es_index,
                        "_type": self.es_doc_type,
                        '_id': sent_obj.id,
                        "_source": {
                            'body': text_body,
                            'body_with_title': '{} \n {}'.format(article.title, text_body),
                            'article_id': article.id,
                            'article_title': article.title,
                            'entities': json.dumps(sent_obj.ents),
                            'noun_chunks': json.dumps(sent_obj.noun_chunks),
                        }
                    }


    def get_chunk_size(self):
        return self.no_items_per_request

    def _is_good_sentence(self, sent):
        # remove sentences that are very short (just do it based on number of characters)
        return len(sent) >= 10

    """
    Filter for good sentences,etc
    """
    def _filter_sents(self,article):
        new_sents = []
        for sent_obj in article.sents:
            if not self._is_good_sentence(sent_obj.text):
                continue

            new_sents.append(sent_obj)
        article.sents = new_sents
        return article

    def create_es_index(self,es_host,es_index_name):
        es = ElasticsearchMagic.get_instance('singleton', hosts=[es_host])

        # delete index if exists
        if es.indices.exists(index=es_index_name):
            es.indices.delete(index=es_index_name)

        settings = {
            "number_of_shards": 9,
            "number_of_replicas": 1,
            "similarity": {
                "default": {
                    "type": "BM25",
                    "k1": 0.1,  # default is 1.2. Value of 0.0 means that it only depends on IDF (not TF).
                    "b": 0.1,  # default is 0.75. Value of 0.0 disables length-normalization.
                }
            },
            "analysis": {
                "filter": {
                    "english_possessive_stemmer": {
                        "name": "possessive_english",
                        "type": "stemmer"
                    },
                    "english_stop": {
                        "stopwords": "_english_",
                        "type": "stop"
                    },
                    "kstem_stemmer": {
                        # kstem is less aggressive than porter, e.g. "dogs" => "dog" in porter, but not in kstem
                        "name": "light_english",
                        "type": "stemmer"
                    },
                    "english_porter_stemmer": {
                        "name": "english",  # porter, see StemmerTokenFilterFactory.java
                        "type": "stemmer"
                    }
                },
                "analyzer": {
                    "porter_eng_analyzer": {
                        # https://stackoverflow.com/questions/33945796/understanding-analyzers-filters-and-queries-in-elasticsearch
                        "filter": [
                            "standard",  # does nothing: https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-standard-tokenfilter.html
                            "asciifolding",
                            "english_possessive_stemmer",
                            "lowercase",
                            "english_stop",
                            "english_porter_stemmer"
                        ],
                        "tokenizer": "standard"
                    },
                    "kstem_eng_analyzer": {
                        "filter": [
                            "standard",
                            "asciifolding",
                            "english_possessive_stemmer",
                            "lowercase",
                            "english_stop",
                            "kstem_stemmer"
                        ],
                        "tokenizer": "standard"
                    },
                    "possessive_english_analyzer": {
                        # no stemming
                        "filter": [
                            "standard",
                            "asciifolding",
                            "english_possessive_stemmer",
                            "lowercase",
                            "english_stop",
                        ],
                        "tokenizer": "standard"
                    },
                    "standard_english_analyzer": {
                        "type": "standard",
                        "stopwords": "_english_"
                    },
                }
            }
        }

        mappings_for_analyzed_text_field = {
            "type": "text",
            "index": True,
            "analyzer": "porter_eng_analyzer",
            "fields": {
                "possessive": {"type": "text", "analyzer": "possessive_english_analyzer"},
                "kstem": {"type": "text", "analyzer": "kstem_eng_analyzer"},
            },
        }

        mappings = {
            "doc": {
                "properties": {
                    "entities": {
                        "type": "text",  # json string
                        "index": False,
                    },
                    "noun_chunks": {
                        "type": "text",  # json string
                        "index": False,
                    },
                    "article_title": {
                        "type": "keyword",
                        "index": False,
                    },
                    "article_id": {
                        "type": "integer",
                        "index": True,
                    },
                    "body": mappings_for_analyzed_text_field,
                    "body_with_title": mappings_for_analyzed_text_field,
                }
            }
        }

        es.indices.create(es_index_name, body=dict(
            mappings=mappings,
            settings=settings))

        es.indices.open(es_index_name)