# -*- coding: utf-8 -*

import json
import Result_pb2


def read_data_csv(filename):
    """
        Loads the content of the data file as specified in the file format.
        Returns a python object.
    """
    with open(filename, 'r') as f:
        return map(
            lambda x: {
                'name': x[0],
                'RollNum': int(x[1]),
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
    return json.dumps(data)


def convert_from_json(data):
    return json.loads(data)


def convert_to_protobuf(data):
    result_proto = Result_pb2.Result()
    for student in data:
        student_proto = result_proto.student.add()
        student_proto.name = student.get('name')
        student_proto.rollNum = int(student.get('RollNum'))
        for course in student.get('CourseMarks'):
            proto_marks = student_proto.marks.add()
            proto_marks.name = course.get('CourseName')
            proto_marks.score = int(course.get('CourseMarks'))
    return result_proto


def convert_from_protobuf(data):
    result_proto = Result_pb2.Result()
    result_proto.ParseFromString(data)
    return result_proto
