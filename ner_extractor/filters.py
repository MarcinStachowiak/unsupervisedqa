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

class BlackListContentFilter(AbstractFilterTask):
    def __init__(self,filename):
        self.filename=filename

    def setup(self):
        with open(self.filename) as f:
            content = f.readlines()
        self.anty_keys= [x.strip().lower() for x in content]

    def filter(self, article):
        lower_text=article.text.lower()
        return all([key not in lower_text for key in self.anty_keys])

