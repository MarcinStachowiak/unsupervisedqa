import os

from datashift import AbstractProcessingTask


class NerGatherer(AbstractProcessingTask):
    def __init__(self, phrase_mode, entity_nchars_min_lim):
        self.phrase_mode = phrase_mode
        self.entity_nchars_min_lim = entity_nchars_min_lim

    def process(self, article):
        phrase_pair_set = self._get_unique_entity_pairs(article)
        return phrase_pair_set

    def _get_unique_entity_pairs(self, article):
        """
        :return: a list of pairs [("0 to 6 years", "DATE"), ...]
        """
        phrase_pair_set = set()
        for sent in article.sents:
            phrase_tuple_lst = sent.get_phrases(self.phrase_mode)

            for (phrase_str, phrase_category) in phrase_tuple_lst:
                if len(phrase_str) < self.entity_nchars_min_lim:
                    # only keep entities that have a minimum number of characters
                    continue
                phrase_pair_set.add((phrase_str, phrase_category))
        return list(phrase_pair_set)
