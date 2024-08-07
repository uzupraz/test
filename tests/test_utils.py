import json
import os
import base64


class TestUtils:

    @staticmethod
    def get_file_content(file_name):
        with open(os.getcwd() + file_name, encoding='utf-8') as json_file:
                return json.load(json_file)


    @staticmethod
    def encode_to_base64(data:str) -> str:
        return base64.b64encode(data.encode('utf-8')).decode('utf-8')
    

    @staticmethod
    def decode_base64(data:str) -> str:
        return base64.b64decode(data).decode('utf-8')