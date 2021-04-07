import json

from datashift import DataPipeline, DefaultTextLineReader, AbstractProcessingTask, AbstractFilterTask, \
    AbstractReduceTask, DefaultTextLineSaver


class JsonToDict(AbstractProcessingTask):
    def process(self, sample):
        return json.loads(sample)


class DictToJson(AbstractProcessingTask):
    def process(self, sample):
        return json.dumps(sample)


class LowercaseProcessor(AbstractProcessingTask):
    def process(self, sample):
        sample['context'] = sample['context'].lower()
        return sample


class FilterTooShort(AbstractFilterTask):
    def __init__(self,min_length):
        self.min_length=min_length

    def filter(self, sample):
        return len(sample['context']) > self.min_length


class CountSamplesPerCategory(AbstractReduceTask):
    def __init__(self, reduced_value_name):
        super().__init__(reduced_value_name=reduced_value_name)

    def reduce_locally(self, samples):
        result = {}
        for sample in samples:
            if sample['category'] in result:
                result[sample['category']] += 1
            else:
                result[sample['category']] = 1
        return result

    def reduce_globally(self, next_local_reduction_gen):
        result = {}
        for local_reduction in next_local_reduction_gen():
            for k, v in local_reduction.items():
                if k in result:
                    result[k] += v
                else:
                    result[k] = v
        return result

class MeanWordsInContext(AbstractReduceTask):
    def __init__(self, reduced_value_name):
        super().__init__(reduced_value_name=reduced_value_name)

    def reduce_locally(self, samples):
        if len(samples)==0:
            return 0
        else:
            return sum([len(s['context'].split()) for s in samples])/len(samples)

    def reduce_globally(self, next_local_reduction_gen):
        values = []
        for local_reduction in next_local_reduction_gen():
            values.append(local_reduction)
        return sum(values)/len(values)

DataPipeline(
    reader=DefaultTextLineReader(input_data_path_pattern='SQuAD_valid.json'),
    saver=DefaultTextLineSaver(output_data_dir_path='out/',
                               output_file_size=20000,
                               output_file_name_prefix='prepared_'),
    output_metadata_file_path='metadata.yaml',
    processing_chunk_size=10000,
    num_workers=1) \
    .process_task(JsonToDict()) \
    .filter_task(FilterTooShort(min_length=150)) \
    .process_task(LowercaseProcessor()) \
    .reduce_task(CountSamplesPerCategory('no_items_per_category')) \
    .reduce_task(MeanWordsInContext('mean_words')) \
    .process_task(DictToJson()) \
    .shift()
