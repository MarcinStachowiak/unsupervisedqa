from datashift import AbstractReduceTask


class CountEntities(AbstractReduceTask):
    def __init__(self, reduced_value_name, num_articles_per_entity_min_lim):
        self.num_articles_per_entity_min_lim = num_articles_per_entity_min_lim
        super().__init__(reduced_value_name)

    def reduce_locally(self, samples):
        local_unique_entities = {}
        for sample in samples:
            if sample not in local_unique_entities:
                local_unique_entities[sample] = 1
            else:
                local_unique_entities[sample] += 1
        return local_unique_entities

    def reduce_globally(self, next_local_reduction_gen):
        global_unique_entities = {}
        for local_reduction in next_local_reduction_gen():
            for key, value in local_reduction.items():
                key = str(key)
                if key not in global_unique_entities:
                    global_unique_entities[key] = 1
                else:
                    global_unique_entities[key] += value
        keys_to_remove = set()
        for k, v in global_unique_entities.items():
            if v < self.num_articles_per_entity_min_lim:
                keys_to_remove.add(k)
        for k in keys_to_remove:
            del global_unique_entities[k]
        return global_unique_entities
