import sys
from mr.worker import KeyValue
import glob
import importlib.util


class MapReduceSequential:
    """
    Sequential MapReduce implementation
    """
    intermediate_kv_pairs = list()
    args = list()
    filenames = list()

    def __init__(self, args: list):
        self.mr_app_class = None
        self.args = args

    def main(self):
        """
        Entry point to this class
        """
        if len(self.args) < 3:
            print(f"Usage: python mrsequential.py <mrapp> <input_files>..\n")
            exit(-1)
        self.set_filenames()
        self.load_plugin()
        self.run_map()
        self.run_reduce()
        self.intermediate_kv_pairs.sort()
        self.run_reduce()

    def print_kv_pairs(self):
        """
        Utility to print data from kv pairs
        """
        for i in self.intermediate_kv_pairs:
            print(f"Key: {i.key}, Val: {i.value}")

    def set_filenames(self):
        """
        Gets file names from list of wildcards/filenames
        """
        unique_filenames = set()
        for filepath in self.args[2:]:
            for i in glob.glob(filepath):
                unique_filenames.add(i)
        self.filenames = list(unique_filenames)

    def load_plugin(self):
        """
        Loads Map and Reduce functions from given string arg for the MapReduce app.
        """
        try:
            spec = importlib.util.spec_from_file_location(self.args[1].split(".")[0], self.args[1] + ".py")
            self.mr_app_class = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.mr_app_class)
        except Exception as e:
            print(f"Error while loading module:\n {e}")
            exit(-1)

    def run_map(self):
        """
        Runs the map and sets the intermediate values to the list
        """
        # Read input files
        for filename in self.filenames:
            try:
                with open(filename, "r") as input_file:
                    file_contents = input_file.read()
            except Exception as e:
                print(f"Error while trying to open input file :\n{e}")
            interim_value = self.mr_app_class.map(KeyValue(filename, file_contents))
            self.intermediate_kv_pairs = interim_value

    def run_reduce(self):
        """
        Runs the custom reduce and writes to file
        """
        keyval_index = 0
        while keyval_index < len(self.intermediate_kv_pairs):
            group_index = keyval_index + 1
            while group_index < len(self.intermediate_kv_pairs) \
                    and self.intermediate_kv_pairs[group_index] == self.intermediate_kv_pairs[keyval_index]:
                group_index += 1
            keyval_group_with_same_key = KeyValue(self.intermediate_kv_pairs[keyval_index].key, list())
            for keyval in self.intermediate_kv_pairs[keyval_index:group_index + 1]:
                keyval_group_with_same_key.value.append(int(keyval.value))
            keyval_index = group_index + 1
            reduced_keyval = self.mr_app_class.reduce(keyval_group_with_same_key)
            with open("mr-out.txt", "a") as output_file:
                output_file.write(f"{reduced_keyval.key} {reduced_keyval.value}\n")


if __name__ == "__main__":
    MapReduceSequential(sys.argv).main()
