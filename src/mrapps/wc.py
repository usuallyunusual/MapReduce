from mr.worker import KeyValue
import re


def map(keyval):
    """
    Custom map
    """
    intermediate_kv = list()
    for word in re.split(r'[ ,|;"]+', keyval.value):
        intermediate_kv.append(KeyValue(word, "1"))
    return intermediate_kv


def reduce(keyval):
    """
    Custom reduce
    """
    return keyval
