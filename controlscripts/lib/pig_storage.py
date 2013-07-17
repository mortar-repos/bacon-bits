import __builtin__
import re
from glob                      import glob
from java.io                   import File
from java.lang                 import Byte
from org.codehaus.jackson      import JsonFactory, JsonParser
from org.codehaus.jackson.map  import ObjectMapper
from org.apache.pig.scripting  import Pig
from org.apache.pig.data       import DataType

DATATYPES = {}
DATATYPES[Byte(DataType.NULL).toString()]       = None
DATATYPES[Byte(DataType.INTEGER).toString()]    = getattr(__builtin__, "int")
DATATYPES[Byte(DataType.LONG).toString()]       = getattr(__builtin__, "long")
DATATYPES[Byte(DataType.FLOAT).toString()]      = getattr(__builtin__, "float")
DATATYPES[Byte(DataType.DOUBLE).toString()]     = getattr(__builtin__, "float")
DATATYPES[Byte(DataType.CHARARRAY).toString()]  = getattr(__builtin__, "unicode")
DATATYPES[Byte(DataType.BYTEARRAY).toString()]  = getattr(__builtin__, "str")
DATATYPES[Byte(DataType.TUPLE).toString()]      = getattr(__builtin__, "tuple")
DATATYPES[Byte(DataType.BAG).toString()]        = getattr(__builtin__, "list")
DATATYPES[Byte(DataType.MAP).toString()]        = getattr(__builtin__, "map")

class FieldSchema:
    def __init__(self, name, dtype, schema=None):
        self.name   = name
        self.dtype  = dtype
        self.schema = schema

class PigStorage:
    # WARNING: PigStorage does not support escaping commas in strings, so we don't check for that here
    
    BAG_SPLIT_PATTERN = re.compile("(?<=\\)),(?=\\()")

    @staticmethod
    def parse_field(f):
        name   = f.get("name")
        dtype  = DATATYPES[str(f.get("type"))]
        if dtype == map:
            raise Exception("Controlscript PigStorage interface does not support reading map fields")
        schema = f.get("schema")
        if schema != None and schema.get("fields") != None:
            schema = [PigStorage.parse_field(f) for f in schema.get("fields")]
        return FieldSchema(name, dtype, schema)

    @staticmethod
    def parse_schema(path):
        mapper = ObjectMapper()
        parser = mapper.getJsonFactory().createJsonParser(File(path))
        tree   = mapper.readTree(parser)
        fields = tree.get("fields")
        return [PigStorage.parse_field(f) for f in fields]

    @staticmethod
    def read_field(field, schema):
        dtype = schema.dtype
        if dtype == tuple:
            return tuple(PigStorage.read_field(f, s) for f, s
                         in zip(field[1:-1].split(","), schema.schema))
        elif dtype == list:
            return [PigStorage.read_field(f, schema.schema[0]) for f
                    in re.split(PigStorage.BAG_SPLIT_PATTERN, field[1:-1])]
        else:
            return dtype(field)

    @staticmethod
    def read_data(path, schema):
        data  = []
        parts = glob(path + "/part-*")
        for part_path in parts:
            part = open(part_path, "r")
            for line in part:
                data.append(tuple(PigStorage.read_field(f, s) for f, s in zip(line.strip().split("\t"), schema)))
            part.close()
        return data

    @staticmethod
    def read_output(path):
        schema = PigStorage.parse_schema(path + "/.pig_schema")
        return PigStorage.read_data(path, schema)
