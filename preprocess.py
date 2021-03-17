import argparse
import os
import re
from bs4 import BeautifulSoup
from datashift import DataPipeline, AbstractProcessingTask, AbstractFilterTask
from datashift.datapipeline import DefaultTextLineReader, DefaultCSVSaver, DefaultCSVReader
from pylatexenc.latex2text import LatexNodes2Text
from langdetect import detect

class TextCleaner(AbstractProcessingTask):
    TAGS_TO_REMOVE=['h1','h2','h3','h4','h5','h6','h7']
    MIN_LENGTH_FOR_TAGS_TO_REMOVE=100

    def setup(self):
        self.latex_cleaner=LatexNodes2Text()

    def clean_text(self,text):
        text  = self.cleanhtml(text)
        text=self.latex_cleaner.latex_to_text(text)
        text = re.sub(r"http\S+", "", text)
        text=text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').strip()
        text = re.sub(' +', ' ', text)
        return text

    def cleanhtml(self,raw_html):
        cleantext=BeautifulSoup(raw_html, "lxml")
        for tag in self.TAGS_TO_REMOVE:
            for s in cleantext.select(tag):
                if len(s.text)<self.MIN_LENGTH_FOR_TAGS_TO_REMOVE:
                    s.extract()
        return cleantext.text


    def process(self, sample):
        sample['title']=self.clean_text(sample['title'])
        sample['description'] = self.clean_text(sample['description'])
        return sample

class LanguageFilter(AbstractFilterTask):
    def __init__(self,lang):
        self.lang=lang

    def filter(self, sample):
        return detect(sample['description'])==self.lang

class MinDescriptionLengthFilter(AbstractFilterTask):
    def __init__(self,min_char_length):
        self.min_char_length=min_char_length

    def filter(self, sample):
        return len(sample['description'])>self.min_char_length

class JointTitleAndDescription(AbstractProcessingTask):
    def process(self, sample):
        sample['document_identifier'] = sample['id']
        sample['document_text']='{}\n\n{}'.format(sample['description'], sample['title'])

        del sample['title']
        del sample['description']
        del sample['id']
        return sample

if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('--input', help='input path for corpus data', required=True)
    argp.add_argument('--output-dir', default="output/", help='', required=True)
    argp.add_argument('--output-file-size', default=2000, type=int, help='', required=True)
    argp.add_argument('--processing-chunk-size', default=2000, type=int, help='')
    argp.add_argument('--workers', default=10, type=int, help='')
    argp.add_argument('--min_chars', default=150, type=int, help='')
    args = argp.parse_args()

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    DataPipeline(
        reader=DefaultCSVReader(input_data_path_pattern=args.input,input_columns=['id','title','description']),
        saver=DefaultCSVSaver(output_data_dir_path=args.output_dir,output_file_size=args.output_file_size,output_file_name_prefix='cleaned'),
        processing_chunk_size=args.processing_chunk_size,
        num_workers=args.workers) \
        .process_task(TextCleaner()) \
        .filter_task(MinDescriptionLengthFilter(min_char_length=args.min_chars)) \
        .filter_task(LanguageFilter('en')) \
        .process_task(JointTitleAndDescription()) \
        .shift()