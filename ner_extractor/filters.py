from datashift import AbstractFilterTask
from langdetect import detect


class MinDescriptionLengthFilter(AbstractFilterTask):
    def __init__(self,min_char_length):
        self.min_char_length=min_char_length

    def filter(self, article):
        return len(article.text)>self.min_char_length

class LanguageFilter(AbstractFilterTask):
    def __init__(self,lang):
        self.lang=lang

    def filter(self, article):
        return detect(article.text)==self.lang