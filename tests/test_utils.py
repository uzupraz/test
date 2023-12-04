import json


class TestUtils:

    @staticmethod
    def get_file_content(file_name):
        with open(file_name, encoding='utf-8') as json_file:
                return json.load(json_file)