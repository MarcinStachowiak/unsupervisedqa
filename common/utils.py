import random
import secrets
import string

import numpy as np


def random_str(N):
    alphabet = string.ascii_lowercase + string.ascii_uppercase + string.digits
    return ''.join(secrets.choice(alphabet) for i in range(N))

class RandomNumberGenerator:
    """
    Need `secrets` for seed because Spark initialize multiple workers at the same time
    that causes multiple RNG or random numbers to be the same!
    """

    def __init__(self):
        self.vanilla = self.get_random_number_generator()
        self.np = self.get_numpy_random_number_generator()

    def get_random_number_generator(self):
        rng = random.Random()
        rng.seed(secrets.randbits(128))
        return rng

    def get_numpy_random_number_generator(self):
        np_rng = np.random.RandomState(seed=secrets.randbits(32))
        return np_rng

def find_all(a_str, sub):
    # https://stackoverflow.com/questions/4664850/find-all-occurrences-of-a-substring-in-python/19720214
    start = 0
    while True:
        start = a_str.find(sub, start)
        if start == -1:
            return
        yield start
        start += len(sub)  # use start += 1 to find overlapping matches