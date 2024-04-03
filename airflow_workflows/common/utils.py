import re

def _camel_case_to_snake_case(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()

def rename_column_camel_case_to_snake_case(columns: list) -> list:
    # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    return [_camel_case_to_snake_case(col) for col in columns]
