# W4111_F19_HW1
1. I included a duplication check in the insert and update functions of CSVdatatable, which raises an exception if a duplication of primary key occurs during update or insert. The last function in csv_table_tests calls update_by_template but results in a duplication, so the program is terminated by an exception.

2. In handling exceptions for the RDB database, I use "try except" to catch errors that occurs when the query is made, printing the error message "error occurs in MySQL."