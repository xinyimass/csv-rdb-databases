from src.CSVDataTable import CSVDataTable
import logging
import os


# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def t_find_by_tmp1():
    tmp = {'nameLast': 'Williams', 'nameFirst': 'Ted'}

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    result = csv_tbl.find_by_template(tmp)
    print(result)
t_find_by_tmp1()

def t_find_by_tmp2():
    tmp = {'nameLast': 'Williams', 'nameFirst': 'Ted'}
    field_list = ["playerID", 'nameLast', 'nameFirst']

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    result = csv_tbl.find_by_template(tmp, field_list)
    print(result)
t_find_by_tmp2()

def t_delete_by_tmp():
    tmp = {'nameLast': 'Williams', 'nameFirst': 'Ted'}
    field_list = ["playerID", 'nameLast', 'nameFirst']

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)
    print(csv_tbl.find_by_template(tmp,field_list))
    result = csv_tbl.delete_by_template(tmp)
    print(result)
    print(csv_tbl.find_by_template(tmp, field_list))

t_delete_by_tmp()

def t_find_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])
    result = csv_tbl.find_by_primary_key(['aaronto01'], ['nameFirst', 'nameLast'])
    print(result)
t_find_by_key()

def t_delete_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])

    print(csv_tbl.find_by_primary_key(['aaronha01']))
    result = csv_tbl.delete_by_key(['aaronha01'])
    print(result)
    print(csv_tbl.find_by_primary_key(['aaronha01']))
t_delete_by_key()

def t_update_by_tmp():
    tmp = {'nameLast': 'Williams', 'nameFirst': 'Ted'}
    new_values = {'birthCity': 'Tokyo', 'deathYear': '2020'}
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])
    print(csv_tbl.find_by_primary_key(['willite01']))
    result = csv_tbl.update_by_template(tmp, new_values)
    print(result)
    print(csv_tbl.find_by_primary_key(['willite01']))
t_update_by_tmp()

def t_update_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])
    print(csv_tbl.find_by_primary_key(['aardsda01']))
    result = csv_tbl.update_by_key(['aardsda01'], {'birthCity': 'Beijing', 'deathYear': '2019'})
    print(result)
    print(csv_tbl.find_by_primary_key(['aardsda01']))
t_update_by_key()

def t_insert():
    new_record = {'playerID': 'newplayer01', 'nameFirst': 'Lexi', 'nameLast': 'Ma'}
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])
    csv_tbl.insert(new_record)
    print(csv_tbl.find_by_primary_key(['newplayer01']))

t_insert()

def t_update_by_tmp_error():
    tmp = {'nameLast': 'Williams', 'nameFirst': 'Ted'}
    new_values = {'playerID': 'aardsda01', 'birthCity': 'Tokyo', 'deathYear': '2020'}
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])
    result = csv_tbl.update_by_template(tmp, new_values)
    print(result)
t_update_by_tmp_error()