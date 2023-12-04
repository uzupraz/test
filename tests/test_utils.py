import json
import functools


class TestUtils:

    @staticmethod
    def get_file_content(file_name):
        with open(file_name, encoding='utf-8') as json_file:
                return json.load(json_file)

    def sub_test(param_list):
        """Decorates a test case to run it as a set of subtests."""

        def decorator(f):

            @functools.wraps(f)
            def wrapped(self):
                for param in param_list:
                    with self.subTest(**param):
                        f(self, **param)

            return wrapped

        return decorator