from org.apache.pig.scripting  import Pig
from pig_storage               import PigStorage

if __name__ == "__main__":
    print PigStorage.read_output("../example_output/pigstorage")
