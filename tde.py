import sql
import codecs
import datetime
import decimal
import argparse
import logging
import sys
import time
from multiprocessing import Queue
from os import path, remove as del_file
from threading import Thread
from tableausdk import Type
from tableausdk.Extract import Extract, TableDefinition, Table, Row


logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s|%(name)s|%(levelname)s|%(message)s')
log = logging.getLogger(path.basename(__file__))
log.setLevel(logging.DEBUG)


class TdeColumn(object):
    def __init__(self, column_name, source_type):
        self.column_name = column_name
        self.source_type = source_type
        self.tde_type = self.get_tde_type(source_type)

    @staticmethod
    def get_tde_type(py_type):
        if py_type is unicode:
            return Type.UNICODE_STRING
        elif py_type is str:
            return Type.CHAR_STRING
        elif py_type is datetime.datetime:
            return Type.DATETIME
        elif py_type is bool:
            return Type.BOOLEAN
        elif py_type is int:
            return Type.INTEGER
        elif py_type is long:
            return Type.INTEGER
        elif py_type is decimal.Decimal:
            return Type.DOUBLE
        elif py_type is float:
            return Type.DOUBLE
        else:
            return Type.UNICODE_STRING


class TdeWriter(object):
    def __init__(self, extract_path):
        try:
            if path.exists(extract_path):
                #import os.remove as del_file
                del_file(extract_path)
            self.extract = Extract(extract_path)
            self.table_definition = TableDefinition()
            self.table = None
            self.tde_columns = list()
        except Exception as ex:
            log.exception(ex.message)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.extract.close()
        except Exception as ex:
            pass

    def write_row(self, row_data):
        tr = self.get_tde_row(self.table_definition, row_data)
        self.table.insert(tr)
        tr.close()

    def set_metadata(self, metadata):
        self.tde_columns = [TdeColumn(c[0], c[1]) for c in metadata]
        self.table_definition = self.get_table_definition(self.tde_columns)
        self.table = self.extract.addTable('Extract', self.table_definition)

    @staticmethod
    def get_tde_row(table_definition, row_data):
        row = Row(table_definition)
        for idx, data in enumerate(row_data):
            t = table_definition.getColumnType(idx)
            try:
                if t is Type.DOUBLE:
                    row.setDouble(idx, data)
                elif t is Type.BOOLEAN:
                    row.setBoolean(idx, data)
                elif t is Type.CHAR_STRING:
                    row.setCharString(idx, data)
                elif t is Type.DATE:
                    row.setDate(idx, data.year, data.month, data.day)
                elif t is Type.DATETIME:
                    row.setDateTime(idx, data.year, data.month, data.day, data.hour, data.minute, data.second, 0)
                elif t is Type.INTEGER:
                    row.setInteger(idx, data)
                elif t is Type.UNICODE_STRING:
                    row.setString(idx, data)
            except Exception as ex:
                row.setNull(idx)
        return row

    @staticmethod
    def get_table_definition(tde_columns):
        td = TableDefinition()
        for c in tde_columns:
            td.addColumn(c.column_name, c.tde_type)
        return td


class TdeGenerator(object):
    def __init__(self, connection_string, sql_file_path, tde_file_path):
        self.connection_string = connection_string
        self.sql_file_path = sql_file_path
        self.tde_file_path = tde_file_path

    def _sql_reader(self, output_queue):
        log.info('Starting SQL Reader...')
        row_count = 0
        try:
            query = self.read_file(self.sql_file_path)
            s = sql.SqlHelper(self.connection_string)
            for row in s.execute_query(query):
                if row_count == 0:
                    output_queue.put(['metadata', s.metadata])
                    log.debug('SQL Reader: put metadata.')
                output_queue.put(['row', row])
                row_count += 1
            log.info('SQL Reader is complete. Rows: {}'.format(row_count))
        except Exception as ex:
            log.exception(ex.message)

    def _tde_writer(self, input_queue):
        log.info('Starting TDE Writer...')
        try:
            row_count = 0
            with TdeWriter(self.tde_file_path) as tde:
                while True:
                    data = input_queue.get()
                    if data is StopIteration:
                        break
                    if data[0] == 'metadata':
                        tde.set_metadata(data[1])
                        log.debug('TDE Extract setting metadata.')
                    elif data[0] == 'row':
                        tde.write_row(data[1])
                        row_count += 1
            log.info('TDE Writer is complete. Rows: {}'.format(row_count))
        except Exception as ex:
            log.exception(ex.message)

    def execute(self):
        start_time = time.time()
        log.info('Starting TDE Generator...')

        data_queue = Queue()
        consumer = Thread(target=self._tde_writer, args=(data_queue,))
        consumer.start()
        producer = Thread(target=self._sql_reader, args=(data_queue,))
        producer.start()

        producer.join()
        data_queue.put(StopIteration)
        consumer.join()

        log.info('Total TDE Generator elapsed time: {}'.format(time.time() - start_time))

    @staticmethod
    def read_file(file_path):
        if not path.exists(file_path):
            raise Exception('File "{}" was not found.'.format(file_path))
        with codecs.open(file_path, 'r', 'utf-8') as f:
            return f.read()


def main(argv):
    parser = argparse.ArgumentParser(prog='tde.py', description='Extracts data from an ODBC connection to a Tableau Data Extract (TDE) file.')
    parser.add_argument('--tde', required=True, metavar='<tde_file_path>', help='The file path to output the TDE file.')
    parser.add_argument('--cn', required=True, metavar='<ODBC_Connection_String>', help='A valid ODBC connection string to connect to the data source')
    parser.add_argument('--sql', required=True, metavar='<sql_script_file_path>', help='The file path to the source SQL (.sql) script.')
    args = vars(parser.parse_args())

    tde = TdeGenerator(args['cn'], args['sql'], args['tde'])
    tde.execute()

if __name__ == '__main__':
    main(sys.argv)
