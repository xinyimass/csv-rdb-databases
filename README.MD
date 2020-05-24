# Modeling the behavior of databases
## Description

This is a homework project designed for us to learn the behavior of SQL databases by asking us to implement functions that model the behavior of a real database with CSV datatables.

## Classes and functions implemented
- `BaseDataTable.py`: a base class with methods that its subclasses must implement.

- `CSVDataTable.py`: This class builds an interface that queries the CSV files in ../Data/Baseball (which is the comma-delimited version of the [Lahman 2019 baseball database](http://www.seanlahman.com/baseball-archive/statistics/)). The query functions implemented are `find_by_template(self, template, field_list=None, limit=None,
offset=None, order_by=None)`,
`delete_by_template(self, template)`,
`update_by_template(self, template, new_values)`, and
`insert(self, new_record)`.

- `RDBDataTable.py`: This class builds an interface that connects to MySQL workbench and queries the [Lahman2019](http://www.seanlahman.com/baseball-archive/statistics/) schema preloaded into MySQL workbench. The query functions implemented are `find_by_template(self, template, field_list=None, limit=None,
offset=None, order_by=None)`,
`delete_by_template(self, template)`,
`update_by_template(self, template, new_values)`, and
`insert(self, new_record)`.