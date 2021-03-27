from datashift import AbstractProcessingTask

from common.models import Article


class TextLineToArticle(AbstractProcessingTask):
    def process(self, sample):
        sample = Article.deserialize_json(sample)
        return sample