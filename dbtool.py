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


def convert_result_to_dic(tup):
    dic_tp = {}
    for field in tup:
        dic_tp[field[0]] = field
    return dic_tp


def create_and_del_tables(cursor, cursor_tp, tables, tables_tp):
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

def del_all_index(cursor,cursor_tp,table):
    cursor.execute("desc %s;" % table)
    fields = convert_result_to_dic(cursor.fetchall())
    cursor_tp.execute("desc %s;" % table)
    fields_tp = convert_result_to_dic(cursor_tp.fetchall())
    cursor.execute("show index from %s" % table)
    indexs = cursor.fetchall()
    cursor_tp.execute("show index from %s" % table)
    indexs_tp = cursor_tp.fetchall()
    for index in indexs:
        if index[2] == "PRIMARY":
            if index[4] not in fields_tp or fields_tp[index[4]][3] != "PRI":
                fields[index[4]] = list(fields[index[4]])
                fields[index[4]][3] = ''
                fields[index[4]][5] = ''
                create_modify_field(cursor,'modify',table,fields[index[4]])
                cursor.execute("alter table %s drop primary key" % table)
        else:
            cursor.execute("alter table %s drop index %s" %(table,index[2]))

    for index in indexs_tp:
        if index[2] != "PRIMARY" and index[4] in fields:
            if str(index[1]) == '1':
                cursor.execute("alter table %s add index %s(%s)"%(table,index[2],index[4]))
            else:
                cursor.execute("alter table %s add unique %s(%s)"%(table,index[2],index[4]))


def create_modify_field(cursor,op,table,field,old_field=[]):
    if op not in ('add','modify'):
        return
    if field[2] == 'NO':
        field[2] = 'not null'
    else:
        field[2] = ''
    if field[4] is None:
        field[4] = ''
    else:
        field[4] = "default '%s'" % field[4]
    if field[3] == 'UNI':
        if op == 'add':
            field[3] = 'unique key'
        else:
            field[3] = ''
    elif field[3] == 'PRI':
        if op == 'add' or old_field[3] != 'PRI':
            field[3] = 'primary key'
        else:
            field[3] = ''
    else:
        field[3] = ''
    sql = "alter table %s %s %s %s %s %s %s %s;" % (
        table,op,field[0] , field[1], field[2], field[4], field[5], field[3])
    cursor.execute(sql)

def upgrade_all_tables(cursor, cursor_tp, tables):
    for table in tables:
        del_all_index(cursor,cursor_tp,table)
        cursor_tp.execute("desc %s;" % table)
        fields_tp = cursor_tp.fetchall()
        cursor.execute("desc %s;" % table)
        fields = convert_result_to_dic(cursor.fetchall())
        for field_name, *other in fields_tp:
            if field_name not in fields:
                create_modify_field(cursor, 'add' , table,[field_name, *other])
            elif (field_name, *other) != fields[field_name]:
                create_modify_field(cursor, 'modify', table,[field_name, *other],fields[field_name])

        if del_other_fields:
            fields_tp =  convert_result_to_dic(fields_tp)
            for key in fields:
                if key not in fields_tp:
                    sql = "alter table %s drop %s" % (table, key)
                    cursor.execute(sql)

try:
    with open("./dbtool.config", 'r', encoding="utf-8") as file_object:
        json_str = file_object.read()
        json_str = re.sub(r"//.*", "", json_str)
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
    with open(sql_file, 'r', encoding="utf-8") as file_object:
        create_sql = file_object.read()
except Exception as e:
    print("配置文件内容错误")
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
            create_and_del_tables(cursor, cursor_tp, tables, tables_tp)
            upgrade_all_tables(cursor, cursor_tp, tables_tp)
except Exception as e:
    print(e)
    quit()
finally:
    conn.close()
    conn_tp.close()
print("数据库升级成功")
