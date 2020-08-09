import re

#Clean column names
def clean_col_name(str_in):
    """Designed for cleaning column names
    Removes trailing .
    Converts ' ' to _
    Converts & to 'and'
    Converts @ to 'at'
    Removes any non alphanumeric characters
    Removes any leading _
    args:
        str_in - str - The column name to be converted
    returns:
        str - the cleaned column name
    """
    str_out = re.sub(r'&', 'And', str_in) #Put _ around & and @ and change to 'and' and 'at'
    str_out = re.sub(r'@', 'At', str_out) #Put _ around & and @ and change to 'and' and 'at'
    str_out = re.sub(r'(?=\w)([A-Z])', r'_\1', str_out).lower() #change from camel case to lower case hiphenated
    str_out = re.sub(r'.*\.', '', str_out) #Remove everything before a .
    str_out = re.sub(r'\s', '_', str_out) #Replace spaces with _
    str_out = re.sub(r'_+', '_', str_out) #Replace spaces with _
    str_out = re.sub(r'[^\d\w_]','', str_out) #Remove all non allowed characters
    str_out = re.sub(r'^_', '', str_out) #Remove leading _
    return str_out

def str_to_float_format(str_in):
    str_out = str(str_in).strip()
    str_out = re.sub(r'[^\.\d]','',str_out)
    str_out = re.sub(r'((?<![\.\d])\d+(\.+\d+)?)',r'\1',str_out)
    if str_out in ['','-']:
        str_out = 0.0
    return float(str_out)

def zero_pad_single(str_in, zeros=1):
    str_out = re.sub(r'(?<![,\.])\b(\d)(?!\d)', r'0\1', str_in)
    return str_out