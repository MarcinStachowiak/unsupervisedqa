from elasticsearch import Elasticsearch


def setup_es(es_host, es_index):
    es = Elasticsearch([es_host])
    # delete index if exists
    if es.indices.exists(index=es_index):
        es.indices.delete(index=es_index)

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
                        "standard",
                        # does nothing: https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-standard-tokenfilter.html
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
                    "type": "text",
                    "index": True,
                },
                "body": mappings_for_analyzed_text_field,
                "body_with_title": mappings_for_analyzed_text_field,
            }
        }
    }

    es.indices.create(es_index, body=dict(
        mappings=mappings,
        settings=settings))

    es.indices.open(es_index)
