from datashift import AbstractReduceTask


class DefaultCounter(AbstractReduceTask):
    def reduce_locally(self, samples):
        return len(samples)

    def reduce_globally(self, local_reductions):
        return sum(local_reductions)

