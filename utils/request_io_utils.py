from decimal import Decimal

class DataTypeUtils:

    @classmethod
    def convert_decimals_to_float_or_int(cls, item):
        """
        Recursively convert all Decimal values in a dictionary to floats.
        """
        if isinstance(item, list):
            return [DataTypeUtils.convert_decimals_to_float_or_int(i) for i in item]
        elif isinstance(item, dict):
            return {k: DataTypeUtils.convert_decimals_to_float_or_int(v) for k, v in item.items()}
        elif isinstance(item, Decimal):
            # Convert to int if the Decimal is equivalent to an int, else convert to float
            return int(item) if item % 1 == 0 else float(item)
        else:
            return item
