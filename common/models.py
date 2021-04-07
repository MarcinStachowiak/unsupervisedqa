import copy
import json
from enum import Enum
from nltk.corpus import stopwords as nltk_stopwords

class PhraseMode(Enum):
    NER_ONLY = 'ner_only'
    ALL = 'all'

class QuestionStyle(Enum):
    """
    If cloze-style is “[FragmentA] [PERSON] [FragmentB]”, then:

    - "Who [FragmentB] [FragmentA]?" - WBA
    - "[FragmentA], who [FragmentB]?" - AWB
    """
    CLOZE_CATEGORY = 'cloze_category_style'
    CLOZE_GENERIC = 'cloze_generic_style'
    TEMPLATE_WBA = 'template_wba_style'
    TEMPLATE_AWB = 'template_awb_style'



class Article:

    def __init__(self):
        pass

    def import_from(self, raw_row):
        self.text = raw_row['text']
        self.id = raw_row['id']
        self.title = raw_row['title']

        self.sents = None

    @classmethod
    def deserialize_json(cls, json_str):
        dct = json.loads(json_str)
        article = Article()
        for k, v in dct.items():
            article.__dict__[k] = v

        if article.sents is None:
            article.sents=[]

        new_sents = []
        for sent in article.sents:
            new_sents.append(Sentence(sent['id'], sent['text'], sent['ents'], sent['noun_chunks']))
        article.sents = new_sents

        return article

    def __repr__(self):
        return str(self.__dict__)

    def jsonify(self,ensure_ascii=False):
        return json.dumps(self.__dict__,ensure_ascii=ensure_ascii)

class Sentence:
    STOPWORDS = set(nltk_stopwords.words('english'))
    def __init__(self, id, text, ents, noun_chunks):
        """
        noun chunks: https://nlp.stanford.edu/software/dependencies_manual.pdf
        """
        self.id = id  # note that this is sentence ID, and not article_id
        self.text = text
        self.ents = [(e[0], e[1]) for e in ents]
        self.noun_chunks = [(e[0], e[1]) for e in noun_chunks]

    def get_phrases(self, phrase_mode):
        if phrase_mode is PhraseMode.NER_ONLY:
            # don't pass it any noun_chunks
            return self._get_phrases(entities=self.ents, noun_chunks=[])
        else:
            return self._get_phrases(entities=self.ents, noun_chunks=self.noun_chunks)


    def _get_phrases(self,entities, noun_chunks):
        """
        :param entities: list of pairs (ent_str, ent_category)
        :param noun_chunks: list of pairs
        """
        phrases = copy.deepcopy(entities)

        ent_str_set = set([ent_str.lower() for ent_str, _ in entities])
        if noun_chunks:
            discard_set = ent_str_set | self.STOPWORDS

        for nc in noun_chunks:
            nc_str, _ = nc  # ensure it's in the correct format (i.e. pairs)
            nc_str_lower = nc_str.lower()

            if nc_str_lower not in discard_set:
                phrases.append(nc)

        return phrases

    def __repr__(self):
        return str(self.__dict__)

    def jsonify(self,ensure_ascii=False):
        return json.dumps(self.__dict__,ensure_ascii=ensure_ascii)


class PhraseObj:
    def __init__(self, phrase_str, phrase_category):
        self.phrase_str = phrase_str
        self.phrase_category = phrase_category

    @classmethod
    def import_from(cls, row):
        """
        Example format: [["0 to 6 years", "DATE"], [5809465, 53318614, 49544471, 27237145, 54568155]]
        """
        phrase_pair = row[0]

        phrase = cls(phrase_pair[0], phrase_pair[1])
        return phrase

    def __repr__(self):
        return str(self.__dict__)

    def jsonify(self,ensure_ascii=False):
        return json.dumps(self.__dict__,ensure_ascii=ensure_ascii)



class QueriesPerArticleObj:
    def __init__(self, article_id=None, article_title=None, article_raw=None, article_phrases=None, filtered_sents=None, phrase=None):
        self.article_id = int(article_id) if article_id is not None else article_id
        self.article_title = article_title
        self.article_raw = article_raw
        self.article_phrases = article_phrases
        self.filtered_sents = filtered_sents
        self.phrase = phrase # answer phrase

    def __repr__(self):
        return str(self.__dict__)

    def jsonify(self,ensure_ascii=False):
        return json.dumps(self.__dict__,ensure_ascii=ensure_ascii)

    @classmethod
    def deserialize_json(cls, json_str):
        dct = json.loads(json_str)
        qpa = QueriesPerArticleObj()
        for k, v in dct.items():
            qpa.__dict__[k] = v

        qpa.article_phrases=[(e[0],e[1]) for e in qpa.article_phrases]

        qpa.phrase=PhraseObj(qpa.phrase['phrase_str'],qpa.phrase['phrase_category'])

        if qpa.filtered_sents is None:
            qpa.filtered_sents = []

        new_sents = []
        for sent in qpa.filtered_sents:
            new_sents.append(Sentence(sent['id'], sent['text'], sent['ents'], sent['noun_chunks']))
        qpa.filtered_sents = new_sents

        return qpa

class DsDatum:
    def __init__(self, qid, styled_questions, context, answers):
        self.qid = qid
        self.styled_questions = styled_questions
        self.context = context
        self.answers = answers
        self.meta = None

    def __repr__(self):
        return str(self.__dict__)

    def jsonify(self):
        return json.dumps(self.__dict__)

    def jsonify_single_style(self, question_style):
        dct = copy.deepcopy(self.__dict__)
        dct['question'] = self.styled_questions[question_style.value]
        del dct['styled_questions']

        return json.dumps(dct)