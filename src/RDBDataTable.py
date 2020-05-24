from src.BaseDataTable import BaseDataTable
import pymysql
import logging
logger = logging.getLogger()

class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    default_connection = pymysql.connect(host='localhost',
                             user='dbuser',
                             password='dbuserdbuser',
                             db='db4111',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


    def run_q(self, sql, args, fetch=True, cur=None, conn=None, commit=True):
        '''
        Helper function to run an SQL statement.

        :param sql: SQL template with placeholders for parameters.
        :param args: Values to pass with statement.
        :param fetch: Execute a fetch and return data.
        :param conn: The database connection to use. The function will use the default if None.
        :param cur: The cursor to use. This is wizard stuff. Do not worry about it for now.
        :param commit: This is wizard stuff. Do not worry about it.

        :return: A tuple of the form (execute response, fetched data)
        '''

        cursor_created = False
        connection_created = False

        try:

            if conn is None:
                connection_created = True
                conn = self.default_connection

            if cur is None:
                cursor_created = True
                cur = conn.cursor()

            if args is not None:
                log_message = cur.mogrify(sql, args)
            else:
                log_message = sql

            logger.debug("Executing SQL = " + log_message)

            res = cur.execute(sql, args)

            if fetch:
                data = cur.fetchall()
            else:
                data = None

            # Do not ask.
            if commit == True:
                conn.commit()

        except Exception as e:
            raise (e)

        return (res, data)

    def __init__(self, db_table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """

        self.table_name = db_table_name
        self._connect_info = RDBDataTable.default_connection
        self.key_columns = key_columns

    def template_to_where_clause(self, template):

        if template is None or template == {}:
            w_clause = None
            args = None
        else:
            terms = []
            args = []
            for k, v in template.items():
                terms.append(k + "=%s")
                args.append(v)

            w_clause = "where " + (" and ".join(terms))

        return (w_clause, args)

    def template_to_set_clause(self, template):
        if template is None or template == {}:
            w_clause = None
            args = None
        else:
            terms = []
            args = []
            for k, v in template.items():
                terms.append(k + "=%s")
                args.append(v)

            w_clause = "set " + (" , ".join(terms))

        return (w_clause, args)

    def show_requested_fields(self, field_list, rows):
        if rows is None:
            return None
        if field_list is None:
            return rows

        result = []

        for r in rows:
            for f in field_list:
                result.append({f: r[f]})
        return result

    def key_to_template(self, key_fields):

        tmp = dict(zip(self.key_columns, key_fields))
        return tmp

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        template = self.key_to_template(key_fields)
        result = self.find_by_template(template, field_list)
        return result
        

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        w_clause, args = self.template_to_where_clause(template)
        query = "select * from " + self. table_name + " " + w_clause

        try:
            result = self.run_q(query, args)
            if field_list is None:
                if len(result[1]) == 0:
                    return None
                else:
                    return result[1]
            else:
                requested_rs = self.show_requested_fields(field_list, result[1])
                return requested_rs

        except:
            print("error in MySQL.")




    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        template = self.key_to_template(key_fields)
        result = self.delete_by_template(template)
        return result

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        num_rows = len(self.find_by_template(template))
        if num_rows is not 0:
            w_clause, args = self.template_to_where_clause(template)
            query = "delete from " + self.table_name + " " + w_clause
            try:
                self.run_q(query, args)
                return num_rows
            except:
                print("error in MySQL.")
        else:
            return 0

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        template = self.key_to_template(key_fields)
        result = self.update_by_template(template, new_values)
        return result


    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        num_rows = len(self.find_by_template(template))
        if num_rows != 0:

            w_clause, args = self.template_to_where_clause(template)
            s_clause, s_args = self.template_to_set_clause(new_values)

            query = "update " + self.table_name + " " + s_clause + " " + w_clause
            args = s_args + args
            try:
                self.run_q(query, args)

                return num_rows

            except:
                print("error occurs in MySQL.")
        else:
            return 0


    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """

        column_terms = []
        for k in new_record.keys():
            column_terms.append(k)
        columns = ','.join(column_terms)

        value_terms = []
        args = []
        for v in new_record.values():
            value_terms.append('%s')
            args.append(v)

        query = "insert into " + self.table_name + "(" + columns + ") " + "values" + "(" + (','.join(value_terms)) + ")"

        try:
            self.run_q(query, args)
        except:
            print('error occurs in MySQL.')



    def get_rows(self):
        return self._rows




