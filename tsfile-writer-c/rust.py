import time
from enum import IntEnum

from cffi import FFI

ffibuilder = FFI()

ffibuilder.cdef("""
    typedef void* schema;
    typedef void* ts_file_writer;
    typedef void* amp_str;
    
    schema schema_simple(char* device_id, char* measurement_id, int data_type, int encoding, int compression);
    void schema_free(schema s);
    
    ts_file_writer file_writer_new(char* filename, schema s);
    ts_file_writer file_writer_write_int32(ts_file_writer, char* device_id, char* measurement_id, int timestamp, int value);
    void file_writer_close(ts_file_writer writer);
""")

lib = ffibuilder.dlopen("../target/release/libtsfile_writer_c.dylib")


class VariableHolder(object):
    def __init__(self):
        self.refs = dict()

    def get(self, s):
        if s not in self.refs:
            self.refs[s] = bytes(s, 'UTF-8')
        return self.refs[s]


class Schema(object):
    def __init__(self, device_id, measurement_id, data_type, encoding, compression) -> None:
        self.variable_holder = VariableHolder()
        self.instance = lib.schema_simple(self.variable_holder.get(device_id), self.variable_holder.get(measurement_id),
                                          data_type, encoding, compression)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.free()

    def free(self):
        """
        If used in a non context-manager way
        :return:
        """
        if self.instance:
            lib.schema_free(self.instance)
            self.instance = None
        else:
            raise RuntimeError("No instance or already destroyed")


class TsFileWriter(object):
    def __init__(self, filename, schema) -> None:
        self.schema = schema
        self.instance = lib.file_writer_new(bytes(filename, 'UTF-8'), schema.instance)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def write_int(self, devide_id, measurement_id, timestamp, value):
        if self.instance:
            lib.file_writer_write_int32(self.instance, self.schema.variable_holder.get(devide_id),
                                        self.schema.variable_holder.get(measurement_id),
                                        timestamp, value)
        else:
            raise RuntimeError("No instance or already destroyed")

    def close(self):
        """
        If used in a non context-manager way
        :return:
        """
        if self.instance:
            lib.file_writer_close(self.instance)
            self.instance = None
        else:
            raise RuntimeError("No instance or already destroyed")


class TSDataType(IntEnum):
    INT32 = 1
    INT64 = 2
    FLOAT = 3


class TSEncoding(IntEnum):
    PLAIN = 0
    TS2DIFF = 4


class CompressionType(IntEnum):
    UNCOMPRESSED = 0
    SNAPPY = 1


if __name__ == '__main__':
    start = time.time()
    with Schema("d1", "s1", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED) as schema:
        with TsFileWriter("test.tsfile", schema) as file_writer:
            for i in range(0, 10000001):
                file_writer.write_int("d1", "s1", i, i)
    end = time.time()
    print(f"Duration: {end - start}")
