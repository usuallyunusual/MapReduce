class KeyValue:
    """
    The key value pair based on which the MapReduce implementation works
    """
    def __init__(self, key: str, value: str):
        if key is None or len(key) == 0:
            raise ValueError("key cannot be None or empty")
        self.key = key
        self.value = value

    def __lt__(self, other):
        return self.key < other.key