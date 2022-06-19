from cffi import FFI

ffibuilder = FFI()

ffibuilder.cdef("""
    typedef void* schema;
    typedef void* ts_file_writer;

    schema schema_simple(char* device_id, char* measurement_id, int data_type, int encoding, int compression);
    void schema_free(schema* schema);
    
    ts_file_writer file_writer_new(char* filename, schema schema);
    ts_file_writer file_writer_write_int32(ts_file_writer, char* device_id, char* measurement_id, int timestamp, int value);
    void file_writer_close(ts_file_writer writer);
""")

lib = ffibuilder.dlopen("../target/debug/libtsfile_writer_c.dylib")


class Schema(object):
    def __init__(self, device_id, measurement_id, data_type, encoding, compression) -> None:
        self.instance = lib.schema_simple(device_id, measurement_id, data_type, encoding, compression)

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
        self.instance = lib.file_writer_new(bytes(filename, 'UTF-8'), schema.instance)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def write_int(self, devide_id, measurement_id, timestamp, value):
        if self.instance:
            lib.file_writer_write_int32(self.instance, bytes(devide_id, 'UTF-8'), bytes(measurement_id, 'UTF-8'),
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


if __name__ == '__main__':
    a = bytes("d1", 'UTF-8')
    b = bytes("s1", 'UTF-8')
    with Schema(a, b, 1, 0, 0) as schema:
        print("Ha")
        with TsFileWriter("test.tsfile", schema) as file_writer:
            file_writer.write_int("d1", "s1", 1, 1)
