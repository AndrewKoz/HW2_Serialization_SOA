import pickle
import json
import xmltodict
from xml_marshaller import xml_marshaller
from google.protobuf.json_format import ParseDict
from google.protobuf.json_format import MessageToDict
from message_pb2 import Message
import fastavro
from io import BytesIO
import yaml
import msgpack

from timeit import timeit
from tabulate import tabulate
import sys

d = {
      'string': """ Lorem ipsum dolor sit amet, consectetur adipiscing 
           elit. Mauris adipiscing adipiscing placerat. 
           Vestibulum augue augue, 
           pellentesque quis sollicitudin id, adipiscing. 
           """,
      'list': [i for i in range(50)],
      'dict': dict((str(i), 'a') for i in iter(range(50))),
      'int': 57,
      'float': 179.0123456
}

message = '''d = {
      'string': """ Lorem ipsum dolor sit amet, consectetur adipiscing 
           elit. Mauris adipiscing adipiscing placerat. 
           Vestibulum augue augue, 
           pellentesque quis sollicitudin id, adipiscing. 
           """,
      'list': [i for i in range(5)],
      'dict': dict((str(i), 'a') for i in iter(range(5))),
      'int': 57,
      'float': 179.0123456
}'''

size_comparison_table = []

# Pickle

test_pickle_serialize = '''
serialized_object = pickle.dumps(d)
'''

setup_pickle = '''
import pickle
serialized_object = pickle.dumps(d)
'''

serialized_object = pickle.dumps(d)
size_comparison_table.append(('pickle', sys.getsizeof(serialized_object)))

test_pickle_deserialize = '''
initial_dict = pickle.loads(serialized_object)
'''

# JSON

test_json_serialize = '''
serialized_object = json.dumps(d)
'''

setup_JSON = '''
import json
serialized_object = json.dumps(d)
'''

serialized_object = json.dumps(d)
size_comparison_table.append(('JSON', sys.getsizeof(serialized_object)))


test_json_deserialize = '''
initial_dict = json.loads(serialized_object)
'''

# XML

test_xml_serialize = '''
serialized_object = xml_marshaller.dumps(d)
'''

setup_XML = '''
from xml_marshaller import xml_marshaller
serialized_object = xml_marshaller.dumps(d)
'''

serialized_object = xml_marshaller.dumps(d)
size_comparison_table.append(('XML', sys.getsizeof(serialized_object)))

test_xml_deserialize = '''
initial_dict = xml_marshaller.loads(serialized_object)
'''

# Google protocol buffers

test_gpb_serialize = '''
serialized_object.SerializeToString()
'''

setup_gpb = '''
from google.protobuf.json_format import ParseDict
from google.protobuf.json_format import MessageToDict
from message_pb2 import Message
serialized_object = ParseDict(d, Message())
src = serialized_object.SerializeToString()
'''

serialized_object = ParseDict(d, Message())
src = serialized_object.SerializeToString()
size_comparison_table.append(('GPB', sys.getsizeof(src)))


test_gpb_deserialize = '''
Message().ParseFromString(src)
'''


# Avro

schema = fastavro.schema.load_schema("message.avsc")

test_avro_serialize = '''
bytes_writer = BytesIO()
fastavro.schemaless_writer(bytes_writer, schema, d)
serialized_object = bytes_writer.getvalue()
'''

setup_avro = '''
import fastavro
from io import BytesIO
schema = fastavro.schema.load_schema("message.avsc")
bytes_writer = BytesIO()
fastavro.schemaless_writer(bytes_writer, schema, d)
serialized_object = bytes_writer.getvalue()
new_bytes_writer = BytesIO()
fastavro.schemaless_writer(new_bytes_writer, schema, d)
bytes_writer.getvalue()
'''

schema = fastavro.schema.load_schema("message.avsc")
bytes_writer = BytesIO()
fastavro.schemaless_writer(bytes_writer, schema, d)
serialized_object = bytes_writer.getvalue()
size_comparison_table.append(('Avro', sys.getsizeof(serialized_object)))

test_avro_deserialize = '''
new_bytes_writer = BytesIO()
new_bytes_writer.write(serialized_object)
new_bytes_writer.seek(0)
initial_dict = fastavro.schemaless_reader(new_bytes_writer, schema)
'''


# YAML

test_yaml_serialize = '''
serialized_object = yaml.dump(d, Dumper=yaml.CDumper)
'''

setup_yaml = '''
import yaml
serialized_object = yaml.dump(d, Dumper=yaml.CDumper)
'''

serialized_object = yaml.dump(d, Dumper=yaml.CDumper)
size_comparison_table.append(('YAML', sys.getsizeof(serialized_object)))

test_yaml_deserialize = '''
initial_dict = yaml.load(serialized_object, Loader=yaml.CLoader)
'''


# MessagePack
test_mp_serialize = '''
serialized_object = msgpack.packb(d)
'''

setup_mp = '''
import msgpack
serialized_object = msgpack.packb(d)
'''

serialized_object = msgpack.packb(d)
size_comparison_table.append(('MP', sys.getsizeof(serialized_object)))

test_mp_deserialize = '''
initial_dict = msgpack.unpackb(serialized_object)
'''

print(size_comparison_table)

loops = 50000
enc_table = []
dec_table = []

print('Running tests on {} loops'.format(loops))

ser_res = float(timeit(test_pickle_serialize, setup='''{}; {}'''.format(message, setup_pickle), number=loops))
deser_res = float(timeit(test_pickle_deserialize, setup='''{}; {}'''.format(message, setup_pickle), number=loops))
enc_table.append(['Pickle', ser_res])
dec_table.append(['Pickle', deser_res])

ser_res = float(timeit(test_json_serialize, setup='''{}; {}'''.format(message, setup_JSON), number=loops))
deser_res = float(timeit(test_json_deserialize, setup='''{}; {}'''.format(message, setup_JSON), number=loops))
enc_table.append(['JSON', ser_res])
dec_table.append(['JSON', deser_res])

ser_res = float(timeit(test_xml_serialize, setup='''{}; {}'''.format(message, setup_XML), number=loops))
deser_res = float(timeit(test_xml_deserialize, setup='''{}; {}'''.format(message, setup_XML), number=loops))
enc_table.append(['XML', ser_res])
dec_table.append(['XML', deser_res])

ser_res = float(timeit(test_gpb_serialize, setup='''{}; {}'''.format(message, setup_gpb), number=loops))
deser_res = float(timeit(test_gpb_deserialize, setup='''{}; {}'''.format(message, setup_gpb), number=loops))
enc_table.append(['GPB', ser_res])
dec_table.append(['GPB', deser_res])

ser_res = float(timeit(test_avro_serialize, setup='''{}; {}'''.format(message, setup_avro), number=loops))
deser_res = float(timeit(test_avro_deserialize, setup='''{}; {}'''.format(message, setup_avro), number=loops))
enc_table.append(['Avro', ser_res])
dec_table.append(['Avro', deser_res])

ser_res = float(timeit(test_yaml_serialize, setup='''{}; {}'''.format(message, setup_yaml), number=loops))
deser_res = float(timeit(test_yaml_deserialize, setup='''{}; {}'''.format(message, setup_yaml), number=loops))
enc_table.append(['YAML', ser_res])
dec_table.append(['YAML', deser_res])

ser_res = float(timeit(test_mp_serialize, setup='''{}; {}'''.format(message, setup_mp), number=loops))
deser_res = float(timeit(test_mp_deserialize, setup='''{}; {}'''.format(message, setup_mp), number=loops))
enc_table.append(['MP', ser_res])
dec_table.append(['MP', deser_res])


enc_table.sort(key=lambda x: x[1])
enc_table.insert(0, ['Package', 'Seconds', 'Size'])

dec_table.sort(key=lambda x: x[1])
dec_table.insert(0, ['Package', 'Seconds'])

print("\nEncoding Test (%d loops)" % loops)
print(tabulate(enc_table, headers="firstrow"))

print("\nDecoding Test (%d loops)" % loops)
print(tabulate(dec_table, headers="firstrow"))

