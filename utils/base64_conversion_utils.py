import json

from base64 import b64decode, b64encode


class Base64ConversionUtils:


    @staticmethod
    def encode_dict(dict: dict[str, any], encoding: str = 'utf-8') -> str:
        """
        Encodes dict to base64 string with provided encoding
        """
        key = json.dumps(dict).encode(encoding)
        return b64encode(key).decode(encoding)
    

    @staticmethod
    def decode_to_dict(data: str, encoding: str = 'utf-8') -> dict[str, any]:
        """
        Decodes str to dict with provided encoding
        """
        return json.loads(b64decode(data).decode(encoding))
