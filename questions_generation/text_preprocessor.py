import string
import copy
import unicodedata
import statistics
import re

import spacy
from nltk.corpus import stopwords as nltk_stopwords
#from distant_supervision.utils import SpacyMagic

STOPWORDS = set(nltk_stopwords.words('english'))
PUNCTS = set(string.punctuation)
DISCARD_WORD_SET = STOPWORDS | PUNCTS | set([''])
ULIM_CHAR_PER_SENTENCE = 500

class TextPreprocessor:

    @staticmethod
    def get_phrases(*, entities, noun_chunks):
        """
        :param entities: list of pairs (ent_str, ent_category)
        :param noun_chunks: list of pairs
        """
        phrases = copy.deepcopy(entities)

        ent_str_set = set([ent_str.lower() for ent_str, _ in entities])
        if noun_chunks:
            discard_set = ent_str_set | STOPWORDS

        for nc in noun_chunks:
            nc_str, _ = nc  # ensure it's in the correct format (i.e. pairs)
            nc_str_lower = nc_str.lower()

            if nc_str_lower not in discard_set:
                phrases.append(nc)

        return phrases

    @staticmethod
    def unicode_normalize(text):
        """
        Resolve different type of unicode encodings.

        e.g. unicodedata.normalize('NFKD', '\u00A0') will return ' '
        """
        return unicodedata.normalize('NFKD', text)

    @classmethod
    def clean_and_tokenize_str(cls, s):
        tokens = set(re.split(r'\W+', s.lower()))
        tokens = tokens - DISCARD_WORD_SET
        return tokens

    def findall_substr(self, substr, full_str):
        """
        Respect word boundaries
        """
        return re.findall(r'\b{}\b'.format(re.escape(substr)), full_str)

    def is_similar(self, sent1, sent2, f1_cutoff, *, discard_stopwords):
        """
        Based on bag of words.

        :param discard_stopwords: remove stopwords and lowercasing
        """
        if sent1 == sent2:
            return True

        if discard_stopwords:
            tokens1 = self.clean_and_tokenize_str(sent1)
            tokens2 = self.clean_and_tokenize_str(sent2)
        else:
            tokens1 = set(sent1.strip().split())
            tokens2 = set(sent2.strip().split())

        eps = 1e-100

        score1 = float(len(tokens1 & tokens2)) / (len(tokens1) + eps)
        score2 = float(len(tokens1 & tokens2)) / (len(tokens2) + eps)

        f1 = statistics.harmonic_mean([score1, score2])
        return f1 >= f1_cutoff

    def normalize_basic(self, text):
        """
        :param text: tokenized text string
        """
        tokens = [w for w in text.lower().split() if w not in DISCARD_WORD_SET]
        return ' ' . join(tokens)