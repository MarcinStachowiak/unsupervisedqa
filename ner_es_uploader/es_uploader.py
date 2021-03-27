import os
from logging import getLogger

from datashift import AbstractProcessingTask
from elasticsearch import helpers, Elasticsearch
import json



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

    def get_chunk_size(self):
        return self.no_items_per_request

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