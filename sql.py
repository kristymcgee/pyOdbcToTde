# -*- coding: utf-8 -*-

import pyodbc


class SqlHelper:
    def __init__(self, connection_string):
        self.__provider__ = None
        self.__server__ = None
        self.__database__ = None
        self.__user__ = None
        self.__password__ = None
        self.__trusted__ = None
        self.__port__ = None
        self.__sslmode__ = None
        self.metadata = None
        self.__connection_string = connection_string
        self.__load_connection_string__(connection_string)

    def execute_query(self, query_string):
        query = query_string
        row_count = 0
        try:
            with self.__get_connection_object__() as conn:
                conn.timeout = 0
                with conn.cursor() as c:
                    c.execute(query)
                    self.metadata = c.description
                    for row in c:
                        yield row
                        row_count += 1
        except pyodbc.Error as ex:
            raise Exception(str(ex.args[-1]))
        except:
            raise

    def execute_non_query(self, query_string):
        rowcount = 0
        try:
            with self.__get_connection_object__() as conn:
                with conn.cursor() as c:
                    rowcount = c.execute(query_string).rowcount
        except pyodbc.Error as ex:
            conn.rollback()
            raise pyodbc.Error(ex.args[-1])
        except:
            raise
        else:
            conn.commit()
        return rowcount

    def __get_connection_object__(self):
        try:
            if self.__trusted__:
                return pyodbc.connect(server=self.__server__, driver=self.__provider__, database=self.__database__,
                                      trusted_connection=self.__trusted__, port=self.__port__, sslmode=self.__sslmode__)
            else:
                return pyodbc.connect(server=self.__server__, driver=self.__provider__, database=self.__database__,
                                      uid=self.__user__, pwd=self.__password__, port=self.__port__, sslmode=self.__sslmode__)
        except:
            raise

    def __load_connection_string__(self, connection_string):
        parts = parse_connection_string(connection_string)

        if 'server' in parts: self.__server__ = parts['server']
        if 'database' in parts: self.__database__ = parts['database']
        if 'uid' in parts: self.__user__ = parts['uid']
        if 'pwd' in parts: self.__password__ = parts['pwd']
        if 'provider' in parts: self.__provider__ = parts['provider']
        if 'port' in parts: self.__port__ = parts['port']
        if 'sslmode' in parts: self.__sslmode__ = parts['sslmode']
        if 'trusted_connection' in parts: self.__trusted__ = parts['trusted_connection']

        #TODO: Validate required connection string parts
        if self.__user__ is not None or self.__password__ is not None:
            if self.__user__ is None or self.__password__ is None:
                raise ValueError('Invalid connection string value: User and password must be specified for non-trusted connections')
            else:
                self.__trusted__ = False


def build_connection_string(provider, server, database, user=None, password=None, port=None, sslmode=None, trusted_connection=None):
    cn_string = 'provider={};server={};database={}'.format(provider, server, database)
    if user is not None:
        cn_string = '{};uid={}'.format(cn_string, user)
    if password is not None:
        cn_string = '{};pwd={}'.format(cn_string, password)
    if port is not None:
        cn_string = '{};port={}'.format(cn_string, port)
    if sslmode is not None:
        cn_string = '{};sslmode={}'.format(cn_string, sslmode)
    if trusted_connection is not None:
        cn_string = '{};trusted_connection={}'.format(cn_string, trusted_connection)
    return cn_string


def parse_connection_string(connection_string):
    parts = {}
    cn_parts = connection_string.split(';')
    for cn_part in cn_parts:
        p = cn_part.split('=')
        parts[p[0]] = p[1]
    return parts
