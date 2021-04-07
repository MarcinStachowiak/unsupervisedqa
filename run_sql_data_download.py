import json

from datashift import AbstractProcessingTask, AbstractFilterTask
from datashift.datapipeline import AbstractReader, AbstractDataBucket, DataPipeline, DefaultTextLineSaver
import psycopg2
import math

from sshtunnel import SSHTunnelForwarder


class PostgresDataBucket(AbstractDataBucket):
    def __init__(self, offset, chunk_size, hostname, username, password, database, data_extraction_query):
        self.offset = offset
        self.chunk_size = chunk_size
        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
        self.data_extraction_query = data_extraction_query + " OFFSET {} LIMIT {}".format(self.offset,self.chunk_size)
        self.used = False

    def setup(self):
        conn = psycopg2.connect(host=self.hostname, dbname=self.database, user=self.username, password=self.password)
        self.cur = conn.cursor()

    def next_data_chunk(self):
        if not self.used:
            self.cur.execute(self.data_extraction_query)
            columns = list(self.cur.description)
            sql_result = self.cur.fetchall()
            results = []
            for row in sql_result:
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col.name] = row[i]
                results.append(row_dict)
            self.used=True
            return results
        else:
            return None

    def __repr__(self):
        print(str(self.__dict__))

    def __str__(self):
        return str(self.__dict__)


class PostgresReader(AbstractReader):
    def __init__(self, hostname, username, password, database, count_query, data_extraction_query):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
        self.count_query = count_query
        self.data_extraction_query = data_extraction_query

    def build_data_buckets(self, _, chunk_size):
        conn = psycopg2.connect(host=self.hostname, dbname=self.database, user=self.username, password=self.password)
        cur = conn.cursor()
        cur.execute(self.count_query)
        total_items = int(cur.fetchone()[0])
        buckets = []
        parts = math.ceil(total_items / chunk_size)
        for n in range(parts):
            buckets.append(PostgresDataBucket(offset=n * chunk_size,
                                              chunk_size=chunk_size if n * chunk_size <= total_items else total_items - n * chunk_size,
                                              hostname=self.hostname,
                                              database=self.database,
                                              username=self.username,
                                              password=self.password,
                                              data_extraction_query=self.data_extraction_query))
        return buckets

class QAObservation:
    def __init__(self, question, context, answer, answer_start_index, is_impossible, category):
        self.answer = answer
        self.question = question
        self.context = context
        self.answer_start_index = answer_start_index
        self.is_impossible = is_impossible
        self.category = category

    def __repr__(self):
        return str(self.__dict__)

    def jsonify(self, ensure_ascii=False):
        return json.dumps(self.__dict__, ensure_ascii=ensure_ascii)


class DictToQAObservation(AbstractProcessingTask):
    def process(self, sample):
        return QAObservation(question=sample['question'],
                             context=sample['context'],
                             answer=sample['selected_text'],
                             answer_start_index=sample['start_offset'],
                             is_impossible=False,
                             category=sample['answer_category'])


class QAObservationToJsonProcessTask(AbstractProcessingTask):
    def process(self, sample):
        return sample.jsonify()

class PreprocessResults(AbstractProcessingTask):
    def _preprocess_text(self,text:str):
        text=text.strip()
        return text

    def process(self, qa_observation):
        qa_observation.question=self._preprocess_text(qa_observation.question)
        qa_observation.answer = self._preprocess_text(qa_observation.answer)
        if qa_observation.answer[-1] in ['.',';',',',':','!','?']:
            qa_observation.answer=qa_observation.answer[:-1]
        return qa_observation

class AdjustTitleAndAnswerStartCategory(AbstractProcessingTask):
    def __init__(self,remove_title):
        self.remove_title=remove_title

    def process(self, qa_observation):
        splitted_context=qa_observation.context.split('\n\n')
        context_body, title = splitted_context
        assert len(splitted_context)==2
        assert qa_observation.answer_start_index<=len(context_body)
        if self.remove_title:
            qa_observation.context = context_body
        else:
            if title[-1] not in ['.',';',',',':','!','?']:
                title_markup=title+'. '
            else:
                title_markup = title + ' '
            qa_observation.context = title_markup+context_body
            qa_observation.answer_start_index+=len(title_markup)
        return qa_observation

class FilterResults(AbstractFilterTask):
    def filter(self, qa_observation):
        if (len(qa_observation.answer)>15 and qa_observation.answer.strip()[0].isupper() and qa_observation.answer.strip()[-1]=='.') or (len(qa_observation.answer)>80 and qa_observation.answer.strip()[0].isupper()):
            print("Incorrect annotation. Seems like a sentence, not an exact fragment. --> {}".format(qa_observation))
            return False
        else:
            return True


if __name__ == '__main__':
    data_extraction_query = """SELECT q.project_id,q.document_id, q.text as question, a.start_offset, q.created_at, a.answer_category,a.selected_text,d.text as context,d.unique_document_name, u.email
FROM questions as q
LEFT JOIN answers a on q.id = a.question_id
LEFT JOIN documents d on q.document_id = d.id
LEFT JOIN users u on a.user_id = u.id
WHERE d.project_id in (19,20,25,26,27,28) and a.selected_text is not NULL and q.text is not NULL""".replace('\n', ' ')

    count_query = """SELECT COUNT(*)
FROM questions as q
LEFT JOIN answers a on q.id = a.question_id
LEFT JOIN documents d on q.document_id = d.id
LEFT JOIN users u on a.user_id = u.id
WHERE d.project_id in (19,20,25,26,27,28) and a.selected_text is not NULL and q.text is not NULL""".replace('\n', ' ')

    server = SSHTunnelForwarder(
        '107.22.222.219',
        ssh_username='m_stachowiak',
        ssh_pkey='sigmoidal.pem',
        remote_bind_address=('annotationtool.cimufsfjwaiq.us-east-1.rds.amazonaws.com', 5432),
        local_bind_address=('0.0.0.0', 5432)
    )
    server.start()

    DataPipeline(
        reader=PostgresReader(hostname='localhost',
                              username='postgres',
                              password='ro34Ko3b24EoCvIN',
                              database='annotationtool',
                              count_query=count_query,
                              data_extraction_query=data_extraction_query),
        processing_chunk_size=500,
        num_workers=1,
        saver=DefaultTextLineSaver(output_data_dir_path='out/',
                                   output_file_size=20000,
                                   output_file_name_prefix='qa_')) \
        .process_task(DictToQAObservation()) \
        .process_task(AdjustTitleAndAnswerStartCategory(remove_title=False)) \
        .filter_task(FilterResults()) \
        .process_task(PreprocessResults())\
        .process_task(QAObservationToJsonProcessTask()) \
        .shift()

    server.stop()
