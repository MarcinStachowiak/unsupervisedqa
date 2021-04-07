import csv
import json
import logging

import yaml
from datashift import AbstractProcessingTask

from common import utils
from common.models import PhraseObj, QueriesPerArticleObj


class EntityToQueriesMapper(AbstractProcessingTask):
    def __init__(self, phrase_mode, ner_statistics_path, ulim_ner, answer_max_num_chars,
                 num_entities_per_article_to_consider, max_words_in_query_sentence, max_num_of_sentences,
                 num_entities_per_article_to_keep, max_words_in_context):
        self.phrase_mode = phrase_mode
        self.ner_statistics_path = ner_statistics_path
        self.ulim_ner = ulim_ner
        self.answer_max_num_chars = answer_max_num_chars
        self.num_entities_per_article_to_consider = num_entities_per_article_to_consider
        self.max_words_in_query_sentence = max_words_in_query_sentence
        self.max_words_in_context = max_words_in_context
        self.max_num_of_sentences = max_num_of_sentences
        self.num_entities_per_article_to_keep = num_entities_per_article_to_keep
        self.ner_statistics = None

    def setup(self):
        ner_set = set()
        with open(self.ner_statistics_path, newline='') as csvfile:
            csv_reader = csv.DictReader(csvfile, quoting=csv.QUOTE_ALL)
            next(csv_reader)
            for row in csv_reader:
                ner_set.add((row['text'], row['type']))
        self.ner_statistics = self._clean_ners(ner_set)

    def _clean_ners(self, ner_set):
        """
        Remove entities/answers that are too long
        """
        logging.info('Number of NER entities/answers before filtering: {}'.format(len(ner_set)))

        new_ner_set = set([(phrase_str, phrase_category)
                           for phrase_str, phrase_category in ner_set
                           if len(phrase_str) <= self.answer_max_num_chars])

        logging.info('Number of NER entities/answers after filtering: {}'.format(len(new_ner_set)))
        return new_ner_set

    def process(self, article):
        return self._get_entity2qpa_list(article)

    def _largest_index_exceeding_ulim_context(self, word_count_arr):
        # compute the largest index that could have approximately max_words_in_context words
        reverse_accum = 0
        for idx in range(len(word_count_arr) - 1, 0, -1):
            reverse_accum += word_count_arr[idx]
            if reverse_accum >= self.max_words_in_context:
                return idx
        return 0

    def _get_valid_context_sentences(self, article, rng):
        """
        0  10
        1  5
        2  6
        3  7

        Let's say max_words_in_context=10. Then The largest_inclusive_idx should be index of 2.
        We then randint(0,2)
        """
        word_count_arr = []
        for sent in article.sents:
            word_count_arr.append(len(sent.text.split()))  # using split() is only approximate because of punctuation

        assert len(word_count_arr) == len(article.sents)

        largest_inclusive_idx = self._largest_index_exceeding_ulim_context(word_count_arr)
        rnd_idx = rng.vanilla.randint(0, largest_inclusive_idx)

        good_sents = []
        accum_nb_words = 0
        for i in range(rnd_idx, len(article.sents)):
            good_sents.append(article.sents[i])
            accum_nb_words += word_count_arr[i]
            if accum_nb_words >= self.max_words_in_context:
                break

        return good_sents

    def _get_all_phrases_from_sentence_list(self, sent_lst):
        phrase_set = set()
        for sent in sent_lst:
            phrase_set.update(sent.get_phrases(self.phrase_mode))
        return list(phrase_set)

    def _get_entity2qpa_list(self, article):
        rng = utils.RandomNumberGenerator()

        # good_sents is contiguous
        good_sents = self._get_valid_context_sentences(article, rng)

        # only use the first N sentences, instead of using article.text
        #article_raw = ' '.join([sent.text for sent in good_sents])
        article_raw=article.text

        article_phrases = self._get_all_phrases_from_sentence_list(good_sents)

        candidate_phrase_pairs = set()
        for sent in good_sents:
            candidate_phrase_pairs.update(sent.get_phrases(self.phrase_mode))

        candidate_phrase_pairs = list(
            candidate_phrase_pairs & self.ner_statistics)  # only keep ones that are in NER list
        # candidate_phrase_pairs = list(candidate_phrase_pairs & ner_broadcast.value)
        rng.vanilla.shuffle(candidate_phrase_pairs)
        candidate_phrase_pairs = candidate_phrase_pairs[:self.num_entities_per_article_to_consider]

        result_lst = []
        for phrase_str, phrase_category in candidate_phrase_pairs:
            # filtered sentences where the "answer" string is in there.
            # Also, keep only ones that have less than X number of words (others are likely an error).
            filtered_sents = [
                s for s in good_sents
                if (phrase_str, phrase_category) in s.get_phrases(self.phrase_mode) and len(
                    s.text.split()) <= self.max_words_in_query_sentence]

            if not filtered_sents:
                continue

            phrase = PhraseObj(phrase_str, phrase_category)

            rng.vanilla.shuffle(filtered_sents)
            filtered_sents = filtered_sents[:self.max_num_of_sentences]  # only randomly take this many sentences

            qpa = QueriesPerArticleObj(
                article_id=article.id,
                article_title=article.title,
                article_raw=article_raw,  # do not use article.text here. We only use first/some N sentences
                article_phrases=article_phrases,
                filtered_sents=filtered_sents,  # filtered sentences where the "answer" string is in there
                phrase=phrase)
            result_lst.append(qpa)

            if len(result_lst) >= self.num_entities_per_article_to_keep:
                break
        return result_lst


class QueriesPerArticleObjToJson(AbstractProcessingTask):
    def process(self, sample):
        return json.dumps(sample, ensure_ascii=False, default=lambda x: x.__dict__)
