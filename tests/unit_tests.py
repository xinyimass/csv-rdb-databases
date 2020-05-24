# I write and test methods one at a time.
# This file contains unit tests of individual methods.

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


def t_load():

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    print("Created table = " + str(csv_tbl))
#t_load()
def t_save():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)
    csv_tbl.save()


#t_save()

def t_find_by_tmp():
    tmp = {'nameLast': 'Williams', 'nameFirst': 'Ted'}

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    result = csv_tbl.find_by_template(tmp)
    print(result)
#t_find_by_tmp()

def t_delete_by_tmp():
    tmp = {'nameLast': 'Williams', 'nameFirst': 'Ted'}

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)
    result = csv_tbl.delete_by_template(tmp)
    print(result)
    #csv_tbl.save()

#t_delete_by_tmp()

def t_key():

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])
    tmp = csv_tbl.key_to_template(['aardsda01'])
    print(tmp)
t_key()

def t_find_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])
    result = csv_tbl.find_by_primary_key(['aaronto01'], ['nameFirst', 'nameLast'])
    print(result)
#t_find_by_key()

def t_delete_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])
    result = csv_tbl.delete_by_key(['aaronha01'])
    #csv_tbl.save()
    print(result)
#t_delete_by_key()

def t_update_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])
    result = csv_tbl.update_by_key(['aardsda01'], {'birthCity': 'Beijing', 'deathYear': '2019'})
    print(result)
    print(csv_tbl.find_by_primary_key(['aardsda01']))
#t_update_by_key()

def t_update_by_tmp():
    tmp = {'nameLast': 'Williams', 'nameFirst': 'Ted'}
    new_values = {'birthCity': 'Tokyo', 'deathYear': '2020'}
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)
    result = csv_tbl.update_by_template(tmp, new_values)
    print(result)
#t_update_by_tmp()

def t_insert():
    new_record = {'playerID': 'newplayer01', 'nameFirst': 'Lexi', 'nameLast': 'Ma'}
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])
    csv_tbl.insert(new_record)
    print(csv_tbl.find_by_primary_key(['newplayer01']))

#t_insert()