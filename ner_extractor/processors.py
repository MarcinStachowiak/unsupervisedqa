import re
import time
import unicodedata

import spacy
from bs4 import BeautifulSoup
from datashift import AbstractProcessingTask
from pylatexenc.latex2text import LatexNodes2Text

from common import utils
from ner_extractor.models import Article


class ElasticsearchEntryToArticle(AbstractProcessingTask):
    def process(self, sample):
        article = Article()
        article.import_from({'id': sample['id'],
                             'title': sample['title'],
                             'text': sample['description']})
        return article


class ArticleTextCleaner(AbstractProcessingTask):
    TAGS_TO_REMOVE = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7']
    MIN_LENGTH_FOR_TAGS_TO_REMOVE = 100

    def setup(self):
        self.latex_cleaner = LatexNodes2Text()

    def clean_text(self, text):
        text = unicodedata.normalize('NFKD', text)
        text = self.cleanhtml(text)
        text = self.latex_cleaner.latex_to_text(text)
        text = re.sub(r"http\S+", "", text)
        text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').strip()
        text = re.sub(' +', ' ', text)
        return text

    def cleanhtml(self, raw_html):
        cleantext = BeautifulSoup(raw_html, "lxml")
        for tag in self.TAGS_TO_REMOVE:
            for s in cleantext.select(tag):
                if len(s.text) < self.MIN_LENGTH_FOR_TAGS_TO_REMOVE:
                    s.extract()
        return cleantext.text

    def process(self, article):
        article.title = self.clean_text(article.title)
        article.text = self.clean_text(article.text)
        return article


class SentenceSplitterAndNerExtractor(AbstractProcessingTask):
    def __init__(self, ulim_char_per_sentence):
        self.ulim_char_per_sentence = ulim_char_per_sentence

    def setup(self):
        self.en_core_web_sm = spacy.load("en_core_web_sm")
        self.en_ner_bc5cdr_md = spacy.load("en_ner_bc5cdr_md")
        self.en_ner_bionlp13cg_md = spacy.load("en_ner_bionlp13cg_md")

    def teardown(self):
        del self.en_core_web_sm
        del self.en_ner_bc5cdr_md
        del self.en_ner_bionlp13cg_md

    def process(self, article):
        sentence_str_lst = self._sent_tokenize(article.text, article.title)
        sentence_structs = []
        for sent_str in sentence_str_lst:
            ents, noun_chunks = self._compute_ner_and_noun_chunks(sent_str, self.ulim_char_per_sentence)
            if len(ents) + len(noun_chunks) == 0:
                continue

            sentence_structs.append(dict(
                id=utils.random_str(16),
                text=sent_str,
                noun_chunks=noun_chunks,
                ents=ents))
        article.sents = sentence_structs
        return article

    def _sent_tokenize(self, raw_text, title):
        text_lst = re.split(r'[\n\r]+', raw_text)
        if title and text_lst[0] == title:
            # remove the first element if is the same as the title
            text_lst = text_lst[1:]

        sentences_agg = []
        for text in text_lst:
            doc = self.en_core_web_sm(text)
            sentences = [sent.text.strip() for sent in doc.sents]
            sentences_agg.extend(sentences)
        return sentences_agg

    def _compute_ner_and_noun_chunks(self, text, ulim_char_per_sentence):
        """
        https://spacy.io/usage/linguistic-features#noun-chunks

        ents: [('today', 'DATE'), ('Patrick', 'PERSON')]
        noun_chunks: e.g. [('Autonomous cars', 'nsubj'), ('insurance liability', 'dobj')]

        :return: (ents, noun_chunks)
        """
        if len(text) > ulim_char_per_sentence:
            return [], []

        ents_dict = dict()
        noun_chunks_dict = dict()

        self._perform_ner_and_add_to_dict(text, self.en_core_web_sm, ents_dict, noun_chunks_dict)
        self._perform_ner_and_add_to_dict(text, self.en_ner_bc5cdr_md, ents_dict, None)
        self._perform_ner_and_add_to_dict(text, self.en_ner_bionlp13cg_md, ents_dict, None)

        ents = sorted(set([(k, v) for k, v in ents_dict.items()]))
        chunks = sorted(set([(k, v) for k, v in noun_chunks_dict.items()]))
        return ents, chunks

    def _perform_ner_and_add_to_dict(self, text, ner_model, ents_dict, noun_chunks_dict):
        doc = ner_model(text)
        if ents_dict is not None:
            for ent in doc.ents:
                ents_dict[ent.text] = ent.label_
        if noun_chunks_dict is not None:
            for nc in doc.noun_chunks:
                noun_chunks_dict[nc.text] = nc.root.dep_


class ArticleToJsonProcessTask(AbstractProcessingTask):
    def process(self, sample):
        return sample.jsonify()
