import pymysql
import json
import copy
import re


class MysqlUpgradeTool:

    def __init__(self):
        self.del_other_tables = False
        self.del_other_fields = False

    def __init_config(self):
        try:
            with open("./dbtool.config", 'r', encoding="utf-8") as file_object:
                json_str = file_object.read()
                json_str = re.sub(r"//.*", "", json_str)
                config = json.loads(json_str)
        except Exception as e:
            print(e)
            return False
        try:
            self.db_config = {
                'host': config['host'],
                'port': config['port'],
                'user': config['user'],
                'password': config['password'],
                'db': config['db'],
                'charset': 'utf8mb4',
            }
            self.db = config['db']
            self.del_other_tables = config['del_other_tables']
            self.del_other_fields = config['del_other_fields']
            sql_file = config['sql_file']
            with open(sql_file, 'r', encoding="utf-8") as file_object:
                self.create_sql = file_object.read()
        except Exception as e:
            print("配置文件内容错误")
            return False
        self.db_config_tp = copy.copy(self.db_config)
        del self.db_config_tp['db']
        return True

    def upgrade(self):
        if not self.__init_config():
            return False

        conn = pymysql.connect(**self.db_config)
        conn_tp = pymysql.connect(**self.db_config_tp)
        try:
            with conn.cursor() as cursor:
                with conn_tp.cursor() as cursor_tp:
                    cursor_tp.execute("drop database if exists %s_tp;" % self.db)
                    cursor_tp.execute("create database %s_tp;" % self.db)
                    cursor_tp.execute("use %s_tp;" % self.db)
                    cursor_tp.execute(self.create_sql)
                    del self.create_sql
                    cursor.execute("show tables;")
                    cursor_tp.execute("show tables;")
                    tables = self.convert_result_to_list(cursor.fetchall())
                    tables_tp = self.convert_result_to_list(cursor_tp.fetchall())
                    cursor_tp.execute("show create table %s" % "t_account;")
                    print(tables, tables_tp)
                    self.create_and_del_tables(cursor, cursor_tp, tables, tables_tp)
                    print(tables, tables_tp)
                    # self.upgrade_all_tables(cursor, cursor_tp, tables_tp)

                    # sql = "select * from %s;"
                    # cursor.execute(sql % ("t_account",))
                    # result = cursor.fetchall()
                    # print(result)
        except Exception as e:
            print(e)
            return False
        finally:
            conn.close()
            conn_tp.close()
        return True

    def convert_result_to_list(self,tup):
        list_tp = []
        for row in tup:
            for col in row:
                list_tp.append(col)
        return list_tp


    def convert_result_to_dic(self,tup):
        dic_tp = {}
        for field in tup:
            dic_tp[field[0]] = field
        return dic_tp


    def create_and_del_tables(self,cursor, cursor_tp, tables, tables_tp):
        for table in tables_tp:
            if table not in tables:
                cursor_tp.execute("show create table %s" % table)
                create_sql = cursor_tp.fetchone()[1]
                cursor.execute(create_sql)
                tables_tp.remove(table)
        if self.del_other_tables:
            for table in tables:
                if table not in tables_tp:
                    cursor.execute("drop table if exists %s;" % table)
                    tables.remove(table)


    def create_one_field(self,cursor, field):
        if field[2] == 'NO':
            field[2] = 'not null'
        else:
            field[2] = ''
        if field[4] is None:
            field[4] = ''
        else:
            field[4] = "default '%s'" % field[4]
        if field[3] == 'UNI':
            field[3] == 'unique key'
        elif field[3] == 'PRI':
            field[3] == 'primary key'
        else:
            field[3] = ''
        sql = "alter table %s add %s %s %s %s %s;" % (
            field[0], field[1], field[2], field[4], field[5], field[3])
        cursor.execute(sql)


    def upgrade_one_field(self,cursor, cursor_tp, field):
        pass


    def upgrade_all_tables(self,cursor, cursor_tp, tables):
        for table in tables:
            cursor_tp.execute("desc %s;" % table)
            fields_tp = cursor_tp.fetchall()
            cursor.execute("desc %s;" % table)
            fields = self.convert_result_to_dic(cursor.fetchall())
            for a, *b in fields_tp:
                if a not in fields:
                    self.create_one_field(cursor, (a, *b))
                elif (a, *b) != fields[a]:
                    self.upgrade_one_field(cursor, cursor_tp, (a, *b))

            if self.del_other_fields:
                fields_tp = self.convert_result_to_dic(fields_tp)
                for key in fields:
                    if key not in fields_tp:
                        sql = "alter table %s drop %s" % (table, key)
                        cursor.execute(sql)

