import sys
sys.path.append("../src")
from src.RDBDataTable import RDBDataTable


key_fields = ["playerID"]
db = RDBDataTable("people", None, key_fields)
key1 = ['coxbi01']
key2 = ['davisch02']
field_list = ["playerID", 'nameLast', 'nameFirst']
tmp1 = {'nameFirst': 'Bill', 'nameLast': 'Cox'}
tmp2 = {'nameFirst': 'Chris', 'nameLast': 'Davis'}
new_values = {'birthCity': 'Tokyo', 'deathYear': '2020'}
new_record = {'playerID': 'newplayer12', 'nameFirst': 'Database', 'nameLast': 'Cool'}
tmp_inserted = {'nameFirst': 'Database', 'nameLast': 'Cool'}

print(db.find_by_template(tmp1, field_list))
print(db.find_by_template(tmp1))
print(db.update_by_template(tmp1, new_values))
print(db.find_by_template(tmp1))
print(db.delete_by_template(tmp1))
print(db.find_by_template(tmp1))

print(db.find_by_primary_key(key2))
print(db.update_by_key(key2, new_values))
print(db.find_by_primary_key(key2))
print(db.delete_by_key(key2))
print(db.find_by_primary_key(key2))

db.insert(new_record)
print(db.find_by_template(tmp_inserted))


