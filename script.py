# -*- coding: utf-8 -*

import json
import argparse
import commands
import time


def compile():
    """
        Compiles proto file to python
    """
    status, output = commands.getstatusoutput(
        "protoc --python_out=. Result.proto"
    )
    if status != 0:
        raise Exception("Failed : " + output)

try:
    import Result_pb2
except ImportError:
    compile()
    import Result_pb2


def write_data_csv_json(data):
    """
        Writes data into the specified file in the file format
    """
    with open('output_json.txt', "w") as f:
        for i in data:
            s = ""
            for x in i.get('CourseMarks'):
                s = s + ":" + x.get('CourseName') + "," + str(x.get('CourseMarks'))
            f.write(i.get('Name') + "," + str(i.get('RollNo')) + s + "\n")


def write_data_csv_protobuf(data):
    """
        Writes data into the specified file in the file format.
    """
    with open('output_protobuf.txt', "wb") as f:
        for i in data.student:
            s = ""
            for x in i.marks:
                s = s + ":" + x.name + "," + str(x.score)
            f.write(i.name + "," + str(i.rollNum) + s + "\n")


def read_data_csv(filename):
    """
        Loads the content of the data file as specified in the file format.
        Returns a python object.
    """
    with open(filename, 'r') as f:
        return map(
            lambda x: {
                'Name': x[0],
                'RollNo': int(x[1]),
                'CourseMarks': map(
                    lambda x: {
                        'CourseName': x[0],
                        'CourseMarks': int(x[1])
                    }, zip(
                        *[iter(
                            x[2:]
                        )] * 2
                    )
                )
            }, map(
                lambda x: x.replace(
                    ',',
                    ':'
                ).split(
                    ':'
                ), map(
                    lambda x: x.strip(),
                    f.readlines()
                )
            )
        )


def convert_to_json(data):
    """
        Dumps a python list of dictionary to string
    """
    return json.dumps(data)


def convert_from_json(data):
    """

        Returns a list of dictionary from string
    """
    return json.loads(data)


def convert_to_protobuf(data):
    """
        Dumps a python list of dictionary to string
    """
    result_proto = Result_pb2.Result()
    for student in data:
        student_proto = result_proto.student.add()
        student_proto.name = student.get('Name')
        student_proto.rollNum = int(student.get('RollNo'))
        for course in student.get('CourseMarks'):
            proto_marks = student_proto.marks.add()
            proto_marks.name = course.get('CourseName')
            proto_marks.score = int(course.get('CourseMarks'))
    return result_proto.SerializeToString()


def convert_from_protobuf(data):
    """
        Returns a Result object from string
    """
    result_proto = Result_pb2.Result()
    result_proto.ParseFromString(data)
    return result_proto


def write_json(data):
    """
        writes into json output file
    """
    with open('result.json', 'w') as f:
        f.write(data)


def read_json(filename):
    """
        reads data string json
    """
    with open(filename, 'r') as f:
        return f.read()


def write_protobuf(data):
    """
        writes into protobuf output file
    """

    with open('result_protobuf', 'wb') as f:
        f.write(data)


def read_protobuf(filename):
    """
        read data string protobuf
    """
    with open(filename, 'rb') as f:
        return f.read()


def main():
    """
        Main function
    """
    parser = argparse.ArgumentParser(description="Compare protobuf and json")
    parser.add_argument(
        "-c",
        "--compile",
        action="store_true",
        help="compile the protobuf"
    )
    parser.add_argument(
        "-s",
        "--serialize",
        action="store_true",
        help="serialize"
    )
    parser.add_argument(
        "-d",
        "--deserialize",
        action="store_true",
        help="deserialize"
    )
    parser.add_argument(
        "-j",
        "--json",
        help="json format"
    )
    parser.add_argument(
        "-p",
        "--protobuf",
        help="protobuf format"
    )
    parser.add_argument(
        "-t",
        "--time",
        action="store_true",
        help="run time tests"
    )
    args = parser.parse_args()
    if args.compile:
        compile()

    serialize_function = None
    deserialize_funtion = None
    start_time = None
    end_time = None
    serialize_time = None
    deserialize_time = None
    input_file = None

    if args.json:
        serialize_function = convert_to_json
        deserialize_funtion = convert_from_json
        input_file = args.json
        dump_to_file = write_json
        load_from_file = read_json
        write_data_csv = write_data_csv_json

    if args.protobuf:
        serialize_function = convert_to_protobuf
        deserialize_funtion = convert_from_protobuf
        input_file = args.protobuf
        dump_to_file = write_protobuf
        load_from_file = read_protobuf
        write_data_csv = write_data_csv_protobuf

    if args.protobuf and args.json:
        raise Exception("Can't run both at same time")

    if args.serialize:
        if serialize_function is None:
            raise Exception("Serialize called without type")
        data = read_data_csv(input_file)
        start_time = time.clock()
        data_str = serialize_function(data)
        end_time = time.clock()
        dump_to_file(data_str)
        serialize_time = end_time - start_time
        print "Serialize Time : " + str(serialize_time)

    if args.deserialize:
        if deserialize_funtion is None:
            raise Exception("Deserialize called without type")
        data = load_from_file(input_file)
        start_time = time.clock()
        data_str = deserialize_funtion(data)
        end_time = time.clock()
        write_data_csv(data_str)
        deserialize_time = end_time - start_time
        print "Deserialize Time : " + str(deserialize_time)

    if args.time:
        if not args.json and not args.protobuf:
            raise Exception("Called without type")
        data = read_data_csv(input_file)
        start_time = time.clock()
        deserialize_funtion(serialize_function(data))
        end_time = time.clock()
        time_len = end_time - start_time
        print "Time taken to Serialize and Deserialize in memory : " + str(time_len)


if __name__ == "__main__":
    main()
