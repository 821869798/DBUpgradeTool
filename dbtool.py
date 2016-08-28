# -*- coding: utf-8 -*-
import pymysql
import json
import copy
import re

def convert_result_to_list(tup):
    list_tp = []
    for row in tup:
        for col in row:
            list_tp.append(col)
    return list_tp

def create_and_del_table(cursor,cursor_tp,tables,tables_tp):
    for table in tables_tp:
        if table not in tables:
            cursor_tp.execute("show create table %s" % table)
            create_sql = cursor_tp.fetchone()[1]
            cursor.execute(create_sql)
            tables_tp.remove(table)


    if del_other_tables:
        for table in tables:
            if table not in tables_tp:
                cursor.execute("drop table if exists %s;" % table)
                tables.remove(table)


try:
    with open("./dbtool.config", 'r',encoding="utf-8") as file_object:
        json_str = file_object.read()
        json_str = re.sub(r"//.*", "",json_str)
        config = json.loads(json_str)
except Exception as e:
    print(e)
    quit()
try:
    db_config = {
        'host': config['host'],
        'port': config['port'],
        'user': config['user'],
        'password': config['password'],
        'db': config['db'],
        'charset': 'utf8mb4',
    }
    del_other_tables = config['del_other_tables']
    del_other_fields = config['del_other_fields']
    sql_file = config['sql_file']
    with open(sql_file, 'r',encoding="utf-8") as file_object:
        create_sql = file_object.read()
except Exception as e:
    print("配置文件格式错误")
    quit()
db_config_tp = copy.copy(db_config)
del db_config_tp['db']

conn = pymysql.connect(**db_config)
conn_tp = pymysql.connect(**db_config_tp)
try:
    with conn.cursor() as cursor:
        with conn_tp.cursor() as cursor_tp:
            cursor_tp.execute("drop database if exists %s_tp;" % config['db'])
            cursor_tp.execute("create database %s_tp;" % config['db'])
            cursor_tp.execute("use %s_tp;" % config['db'])
            cursor_tp.execute(create_sql)
            del create_sql
            cursor.execute("show tables;")
            cursor_tp.execute("show tables;")
            tables = convert_result_to_list(cursor.fetchall())
            tables_tp = convert_result_to_list(cursor_tp.fetchall())
            cursor_tp.execute("show create table %s" % "t_account")
            print(tables,tables_tp)
            create_and_del_table(cursor,cursor_tp,tables,tables_tp)

            print(tables,tables_tp)
            # sql = "select * from %s;"
            # cursor.execute(sql % ("t_account",))
            # result = cursor.fetchall()
            # print(result)
except Exception as e:
    print(e)
    quit()
finally:
    conn.close()
    conn_tp.close()

