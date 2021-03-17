import secrets
import string


def random_str(N):
    alphabet = string.ascii_lowercase + string.ascii_uppercase + string.digits
    return ''.join(secrets.choice(alphabet) for i in range(N))