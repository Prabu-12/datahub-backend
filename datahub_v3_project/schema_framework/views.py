from django.shortcuts import render
from rest_framework.views import APIView
#from.models import pipe_line
#from .serializers import pipe_lineserializer
from django.http.response import Http404
from urllib import response
from rest_framework.response import Response
from pipeline_details_api.serializers import *
from django.db.models import Count
import json
import requests
import pyodbc
from datahub_v3_app.models import *
from pipeline_framework.serializers import *
from pipeline_schedule_api.serializers import *
from django.utils import timezone
import pandas as pd
import snowflake.connector
import mysql.connector as msql
from snowflake.connector.pandas_tools import write_pandas
import psycopg2 as ps
import teradatasql
import boto3
import cx_Oracle
import json
import sqlite3
import csv


def schema_fram(schema_id):
        # import pdb
        # pdb.set_trace()
        print(schema_id)
        current_date=timezone.now().date()
        mssg_schema_mig_t='validation sucessfull in schema migration'
        mssg_congig_t='validation sucessfull in pipeline configuration'
        mssg_connection_detail_source_t='validation sucessfull in connection detail source'
        mssg_connection_detail_target_t='validation sucessfull in connection detail target'
        mssg_conection_source_t='validation sucessfull in connection source'
        mssg_connection_target_t='validation sucessfull in connection target'

        mssg_schema_mig_t='validation failed in schema migration'
        mssg_congig_f='validation failed in pipeline configuration'
        mssg_connection_detail_source_f='validation failed in connection detail source'
        mssg_connection_detail_target_f='validation failed in connection detail target'
        mssg_conection_source_f='validation failed in connection source'
        mssg_connection_target_f='validation failed in connection target'

        migraton_set=schema_migration.objects.filter(id=schema_id).values()
        temp_migration={}
        for i in migraton_set:
                temp_migration.update(i)
        
        config_set=db_config.objects.filter(id=temp_migration['config_id_id']).values()
        temp_config={}
        for i in config_set:
                temp_config.update(i)

        source_connection=connection_detail.objects.filter(id=temp_config['Source_conn_det_id_id']).values()
        temp_source={}
        for i in source_connection:
                temp_source.update(i)

        target_connection=connection_detail.objects.filter(id=temp_config['Target_conn_det_id_id']).values()
        temp_target={}
        for i in target_connection:
                temp_target.update(i)

        source_connection_id=conn.objects.filter(id=temp_source['connection_id_id']).values()
        temp_souconnection_name={}
        for i in source_connection_id:
                temp_souconnection_name.update(i)

        target_connection_id=conn.objects.filter(id=temp_target['connection_id_id']).values()
        temp_tarconnection_name={}
        for i in target_connection_id:
                temp_tarconnection_name.update(i)
        # schedule id posting
        post_value_slog=schedule_log(schedule_id=temp_migration['id'],pipeline_id=temp_config['id'],status='running',level='schema')
        
        post_value_slog.save() 
        # getting run id

        run_id= schedule_log.objects.filter(schedule_id=temp_migration['id']).values('run_id')
        temp_rid={}
        for i in run_id:
                temp_rid.update(i)
        print(run_id)
        run_ids=temp_rid['run_id']


       
        if temp_migration['id']>= 1 and temp_migration['is_active'] == 1 and temp_migration['start_date']<=current_date and temp_migration['end_date']>=current_date :
                audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_schema_mig_t)
                audit_los_save.save()
                if temp_config['id']>= 1 and temp_config['is_active'] == 1 and temp_config['start_date']<=current_date and temp_config['end_date']>=current_date :
                        audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_congig_t)
                        audit_los_save.save()
                        if temp_source['id']>= 1 and temp_source['is_active'] == 1 and temp_source['start_date']<=current_date and temp_source['end_date']>=current_date :
                                audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_detail_source_t)
                                audit_los_save.save()
                                if temp_target['id']>= 1 and temp_target['is_active'] == 1 and temp_target['start_date']<=current_date and temp_target['end_date']>=current_date :
                                        audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_detail_target_t)
                                        audit_los_save.save()
                                        if temp_souconnection_name['id']>= 1 and temp_souconnection_name['is_active'] == 1 and temp_souconnection_name['start_date']<=current_date and temp_souconnection_name['end_date']>=current_date :
                                                audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_conection_source_t)
                                                audit_los_save.save()
                                                if temp_tarconnection_name['id']>= 1 and temp_tarconnection_name['is_active'] == 1 and temp_tarconnection_name['start_date']<=current_date and temp_tarconnection_name['end_date']>=current_date :
                                                        audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_target_t)
                                                        audit_los_save.save()

                                                        dataset=[]
                                                        target={}
                                                        sources={}
                                                        sources.update({"source_connection_id":temp_souconnection_name['id']})
                                                        sources.update(temp_source['con_str'])
                                                        sources.update({"run_id":temp_rid['run_id']})
                                                        sources.update({"source_table_name":temp_migration['schema_name']})
                                                        #target.update({"target_table_name":temp_pipe['target_table_name']})
                                                        sources.update({"migration_id":schema_id})
                                                        sources.update({"target_connection_id":temp_tarconnection_name['id']})
                                                        target.update({"target_connection_id":temp_tarconnection_name['id']})
                                                        target.update(temp_target['con_str'])
                                                        target.update({"target_table_name":temp_migration['schema_name']})
                                                        target.update({"run_id":temp_rid['run_id']})
                                                        target.update({"migration_id":schema_id})
                                                
                                                        dataset.append(sources)
                                                        dataset.append(target)
                                                        print(sources)
                                                        print(target)
                                                        print(dataset)
                                                        wraper_script(dataset=dataset)
                                                        
                                                        return (dataset)
                                                else:
                                                        audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_target_f)
                                                        audit_los_save.save()
                                                        error_log_save=error_log(run_id=temp_rid['run_id'],schedule_id=1,status=mssg_connection_target_f)
                                                        error_log_save.save()
                                                        put(run_ids)
                                        else:
                                                audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_conection_source_f)
                                                audit_los_save.save()
                                                error_log_save=error_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_conection_source_f)
                                                error_log_save.save()
                                                put(run_ids)
                                else:
                                        audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_detail_target_f)
                                        audit_los_save.save()
                                        error_log_save=error_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_detail_target_f)
                                        error_log_save.save()
                                        put(run_ids)
                        else:
                                audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_detail_source_f)
                                audit_los_save.save()
                                error_log_save=error_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_detail_source_f)
                                error_log_save.save()
                                put(run_ids)
                else:
                        audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_congig_f)
                        audit_los_save.save()
                        error_log_save=error_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_congig_f)
                        error_log_save.save()
                        put(run_ids)
        else:
                audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_schema_mig_t)
                audit_los_save.save()
                error_log_save=error_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_schema_mig_t)
                error_log_save.save()
                put(run_ids)
def create_table(dataset):
        # import pdb
        # pdb.set_trace()
        sources=dataset[1]
        target=dataset[0]
        result = migration_log.objects.values('table_name').filter(schema_migration_id=53).annotate(dcount=Count('column_name'))
        print(result)
        conn = snowflake.connector.connect(
                user=sources['user'],
                password=sources['password'],
                account=sources['account'],
                warehouse=sources['warehouse'],
                database=sources['database'],
                schema=sources['schema'],
                role=sources['role']
                )
        cur=conn.cursor()
        print('connecton succes')
        for i in result:
                table=i['table_name']
                tbl=migration_log.objects.filter(table_name=table).values('table_name','column_name','data_type')
                print(tbl)
                create_statement=create_tbl(i=tbl)
                remove_brace=str(create_statement)[1:-1]
                final_create_statement=remove_brace.replace("'","")
                sql =f'''CREATE TABLE {table}({final_create_statement})'''
                print(sql)
                cur.execute(sql)
                conn.commit()
        return'hii'


               
def put(run_ids):
        qset=schedule_log.objects.get(run_id=run_ids)
        qset.status=("failed")
        qset.save()
        return Response(qset)

def put_c(run_ids):
        qset=schedule_log.objects.get(run_id=run_ids)
        qset.status=("completed")
        qset.save()
        return Response(qset)
def schema_mapping(sources):
        # import pdb
        # pdb.set_trace()
        print(sources)
        source_connection_id=sources['source_connection_id']
        if source_connection_id == 1:
                table_name=schema_sf(sources=sources)
        elif source_connection_id == 2:
                table_name=schema_pg(sources=sources)
        elif source_connection_id == 3:
                table_name=schema_mssql(sources=sources)
        elif source_connection_id == 4:
                table_name=source_teradata(sources=sources)
        elif source_connection_id == 5:
                table_name=source_teradata(sources=sources)
        elif source_connection_id == 6:
                table_name=schema_sqlserver(sources=sources)
        elif source_connection_id == 7:
                table_name=source_sql_lite(sources=sources)
        
        return table_name

def create_table_wraper(target):
        print(target)
        target_connection_id=target['target_connection_id']
        if target_connection_id == 1:
                create_tbl_sf(target=target)  
        elif target_connection_id == 2:
                create_tbl_pg(target=target)
        elif target_connection_id == 3:
                create_tbl_msql(target=target)
        elif target_connection_id == 4:
                create_tbl_ora(target=target)
        elif target_connection_id == 5:
                create_tbl_tera(target=target)
        elif target_connection_id == 6:
                create_tbl_sqlserver(target=target)



def wraper_script(dataset):
        # import pdb
        # pdb.set_trace()
        print(dataset)
        sources=dataset[0]
        target=dataset[1]
        print(sources)
        print(target)
        # import pdb
        # pdb.set_trace()
        table_name=schema_mapping(sources=sources)
        create_table_wraper(target=target)
        result = migration_log.objects.values('table_name').filter(schema_migration_id=sources['migration_id']).annotate(dcount=Count('column_name'))
        print(result)
        for table  in result:
                # import pdb
                # pdb.set_trace()
                sources.update({'source_table_name':table['table_name']})
                target.update({'target_table_name':table['table_name']})
                data_set=source_mapping(sources=sources)
                print("hii iam reurning set")
                print(data_set)
                target_mapping(target=target,data_set=data_set)
       
        run_ids=sources['run_id']
        # import pdb
        # pdb.set_trace()
        put_c(run_ids)

def source_mapping(sources):
       
        print(sources)
        source_connection_id=sources['source_connection_id']
        if source_connection_id == 1:
                data_set=source_sf(sources=sources)
        elif source_connection_id == 2:
                data_set=source_postgres(sources=sources)
        elif source_connection_id == 3:
                data_set=source_mysql(sources=sources)
        elif source_connection_id == 4:
                data_set=source_oracle(sources=sources)
        elif source_connection_id == 5:
                data_set=source_teradata(sources=sources)
        elif source_connection_id == 6:
                data_set=source_sqlserver(sources=sources)
        return data_set
        

def target_mapping(target,data_set):
        print(target)
        target_connection_id=target['target_connection_id']
        if target_connection_id == 1:
                target_sf(target=target,data_set=data_set)  
        elif target_connection_id == 2:
                target_postgres(target=target,data_set=data_set)
        elif target_connection_id == 3:
                target_mysql(target=target,data_set=data_set)
        elif target_connection_id == 4:
                target_oracle(target=target,data_set=data_set)
        elif target_connection_id == 5:
                target_teradata(target=target,data_set=data_set)
        elif target_connection_id == 6:
                target_sqlserver(target=target,data_set=data_set)


#schema mapping
def schema_pg(sources):
        try:

                # establishing the connection
                conn = ps.connect(
                        database=sources['database'],
                        user=sources['user'],
                        password=sources['password'],
                        host=sources['host'],
                        port= sources['port']
                )
                cur = conn.cursor()
                # import pdb
                # pdb.set_trace()

                sql='''select table_name,column_name,data_type,ordinal_position  from information_schema.columns where table_schema='public'order by table_name,columns.ordinal_position;'''
                #cur2.execute(database)
                cur.execute(sql)
                df=pd.DataFrame(cur)
                for i in df.values.tolist():
                        schema_save=migration_log(schema_migration_id=sources['migration_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=sources['source_table_name'],column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0],row_count=100)
                        schema_save.save()
                df = pd.DataFrame(cur)
                
                print(df)
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def schema_sf(sources):
        try:
                print('Connecting to te sf database...')
                conn = snowflake.connector.connect(
                user=sources['user'],
                password=sources['password'],
                account=sources['account'],
                warehouse=sources['warehouse'],
                database=sources['database'],
                schema=sources['schema'],
                role=sources['role']
                )
                print("Connection successful")
                print('hi')
                #print
                cur = conn.cursor()
                
                sql='''select distinct a.table_name,a.column_name,a.data_type,a.ordinal_position,b.row_count from information_schema.columns
                a join information_schema.tables b on a.table_name=b.table_name where table_type = 'BASE TABLE' order by a.table_name,a.ordinal_position ;'''
                #cur2.execute(database)
                cur.execute(sql)
                df=pd.DataFrame(cur)
                for i in df.values.tolist():
                        schema_save=migration_log(schema_migration_id=sources['migration_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=sources['schema'],row_count=i[4],column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0])
                        schema_save.save()
                #table_names=df.values.tolist()
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return 'end'
def schema_mssql(sources):
        try:

                print('connecting..')
                scnn = msql.connect(host=sources['host'],
                                port=sources['port'],
                                user=sources['user'],
                                password=sources['password'],
                                database=sources['database']
                                )
                cs = scnn.cursor()
                table={'name':'DATAHUB2'}
                sql= f"SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='{table['name']}';  "
                cs.execute(sql)
                df = pd.DataFrame(cs)
                for i in df.values.tolist():
                        schema_save=migration_log(schema_migration_id=sources['migration_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=sources['schema'],row_count=i[4],column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0])
                        schema_save.save()
                print(df)
                print('completed..')
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def schema_sqlserver(sources):

        try:

                # import pdb
                # pdb.set_trace()
                print('Connecting to the PostgresSQL database...')
                conn = pyodbc.connect(DRIVER=sources['driver'],
                                SERVER=sources['server'],
                                PORT=sources['port'],
                                DATABASE=sources['database'],
                                UID=sources['username'],
                                PWD=sources['password'])
                print("Connection successful")
                cur = conn.cursor()
                sql = "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE,ORDINAL_POSITION,TABLE_SCHEMA FROM information_schema.COLUMNS;"

                df = pd.read_sql_query(sql, conn)
                for i in df.values.tolist():
                                schema_save=migration_log(schema_migration_id=sources['migration_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=i[4],row_count=100,column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0])
                                schema_save.save()
                print(df)

        except (Exception, pyodbc.DatabaseError) as error:
                print(error)
        
        

def source_teradata(sources):

        try:
                # import pdb
                # pdb.set_trace()
                conn = teradatasql.connect(
                        host=sources['server'],
                        user=sources['username'],
                        password=sources['password'],
                        encryptdata='true',
                )
                cur=conn.cursor()
                databases=sources['database']
                print("Connection successful")
                sql = """select TableName,DataBaseName,ColumnName,ColumnLength,ColumnType,ColumnId,CreatorName 
                from columns where databasename={databases}"""
                df = pd.read_sql_query(sql, conn)
                for i in df.values.tolist():
                        schema_save=migration_log(schedule_id=sources['schedule_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=sources['schema'],row_count=i[4],column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0])
                        schema_save.save()
                print(type(df))
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df




def create_tbl_sf(target):
        try:
                # import pdb
                # pdb.set_trace()
                
                result = migration_log.objects.values('table_name').filter(schema_migration_id=target['migration_id']).annotate(dcount=Count('column_name'))
                print(result)
                conn = snowflake.connector.connect(
                        user=target['user'],
                        password=target['password'],
                        account=target['account'],
                        warehouse=target['warehouse'],
                        database=target['database'],
                        schema=target['schema'],
                        role=target['role']
                        )
                cur=conn.cursor()
                print('connecton success')
                for i in result:
                        table=i['table_name']
                        tbl=migration_log.objects.filter(table_name=table).filter(schema_migration_id=target['migration_id']).values('table_name','column_name','data_type')
                        print(tbl)
                        create_statement=create_tbl(i=tbl)
                        remove_brace=str(create_statement)[1:-1]
                        final_create_statement=remove_brace.replace("'","")
                        sql =f'''CREATE TABLE {table}({final_create_statement})'''
                        print(sql)
                        cur.execute(sql)
                        conn.commit()
                        print('Table created Successfully'+ table)
        except (Exception, ps.DatabaseError) as error:
                print(error)
def create_tbl_pg(target):
        try:
                # import pdb
                # pdb.set_trace()
                
                result = migration_log.objects.values('table_name').filter(schema_migration_id=target['migration_id']).annotate(dcount=Count('column_name'))
                print(result)
                conn = ps.connect(
                        database=target['database'],
                        user=target['user'],
                        password=target['password'],
                        host=target['host'],
                        port= target['port']
                )
                cur = conn.cursor()
                print('connecton success')
                for i in result:
                        table=i['table_name']
                        tbl=migration_log.objects.filter(table_name=table).filter(schema_migration_id=target['migration_id']).values('table_name','column_name','data_type')
                        print(tbl)
                        create_statement=create_tbl(i=tbl)
                        remove_brace=str(create_statement)[1:-1]
                        final_create_statement=remove_brace.replace("'","")
                        sql =f'''CREATE TABLE {table}({final_create_statement})'''
                        print(sql)
                        cur.execute(sql)
                        conn.commit()
                conn.close()
        except (Exception, ps.DatabaseError) as error:
                print(error)
def create_tbl_msql(target):
        try:
                # import pdb
                # pdb.set_trace()
                
                result = migration_log.objects.values('table_name').filter(schema_migration_id=target['migration_id']).annotate(dcount=Count('column_name'))
                print(result)
                
                conn = msql.connect(host=target['host'],
                                        user=target['user'],
                                        password=target['password'],
                                        port=target['port'],
                                        database=target['database']
                                        )
                cur = conn.cursor()
                print('connecton success')
                for i in result:
                        table=i['table_name']
                        tbl=migration_log.objects.filter(table_name=table).filter(schema_migration_id=target['migration_id']).values('table_name','column_name','data_type')
                        print(tbl)
                        create_statement=create_tbl(i=tbl)
                        remove_brace=str(create_statement)[1:-1]
                        final_create_statement=remove_brace.replace("'","")
                        sql =f'''CREATE TABLE {table}({final_create_statement})'''
                        print(sql)
                        cur.execute(sql)
                        conn.commit()
                conn.close()
        except (Exception, ps.DatabaseError) as error:
                print(error)
def create_tbl_ora(target):
        try:
                # import pdb
                # pdb.set_trace()
                
                result = migration_log.objects.values('table_name').filter(schema_migration_id=target['migration_id']).annotate(dcount=Count('column_name'))
                print(result)
                connstr =f"{target['user']}/{target['password']}@{target['host']}:{target['port']}/{target['database']}"

                conn = cx_Oracle.connect(connstr)
                cur = connstr.cursor()
                print("connected")

                for i in result:
                        table=i['table_name']
                        tbl=migration_log.objects.filter(table_name=table).filter(schema_migration_id=target['migration_id']).values('table_name','column_name','data_type')
                        print(tbl)
                        create_statement=create_tbl(i=tbl)
                        remove_brace=str(create_statement)[1:-1]
                        final_create_statement=remove_brace.replace("'","")
                        sql =f'''CREATE TABLE {table}({final_create_statement})'''
                        print(sql)
                        cur.execute(sql)
                        conn.commit()
        except (Exception, ps.DatabaseError) as error:
                print(error)
def create_tbl_tera(target):
        try:
                # import pdb
                # pdb.set_trace()
                
                result = migration_log.objects.values('table_name').filter(schema_migration_id=target['migration_id']).annotate(dcount=Count('column_name'))
                print(result)
                conn = teradatasql.connect(
                        host=target['server'],
                        user=target['username'],
                        password=target['password'],
                        encryptdata='true',
                )
                cur = conn.cursor()
                print("teradata connected")
                for i in result:
                        table=i['table_name']
                        tbl=migration_log.objects.filter(table_name=table).filter(schema_migration_id=target['migration_id']).values('table_name','column_name','data_type')
                        print(tbl)
                        create_statement=create_tbl(i=tbl)
                        remove_brace=str(create_statement)[1:-1]
                        final_create_statement=remove_brace.replace("'","")
                        sql =f'''CREATE TABLE {table}({final_create_statement})'''
                        print(sql)
                        cur.execute(sql)
                        conn.commit()
        except (Exception, ps.DatabaseError) as error:
                print(error)
def create_tbl_sqlserver(target):
        try:
                # import pdb
                # pdb.set_trace()
                
                result = migration_log.objects.values('table_name').filter(schema_migration_id=target['migration_id']).annotate(dcount=Count('column_name'))
                print(result)
                conn = pyodbc.connect(DRIVER=target['driver'],
                SERVER=target['server'],
                PORT=target['port'],
                DATABASE=target['database'],
                UID=target['user'],
                PWD=target['password'])
                cur = conn.cursor()
                print("teradata connected")
                for i in result:
                        table=i['table_name']
                        tbl=migration_log.objects.filter(table_name=table).filter(schema_migration_id=target['migration_id']).values('table_name','column_name','data_type')
                        print(tbl)
                        create_statement=create_tbl_sqlserver(i=tbl)
                        remove_brace=str(create_statement)[1:-1]
                        final_create_statement=remove_brace.replace("'","")
                        sql =f'''CREATE TABLE {table}({final_create_statement})'''
                        print(sql)
                        cur.execute(sql)
                        conn.commit()
        except (Exception, ps.DatabaseError) as error:
                print(error)

                                                                        
def source_postgres(sources):
        try:

                # establishing the connection
                conn = ps.connect(
                        database=sources['database'],
                user=sources['user'],
                password=sources['password'],
                host=sources['host'],
                port= sources['port']
                )
                cur = conn.cursor()


                sql = f"select * from {sources['source_table_name']}"
                cur.execute(sql)
                df = pd.read_sql_query(sql,conn)
                
                print('read the table' ,df)
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df
      


def target_postgres(target,data_set):
        try:
                # establishing the connection
                conn = ps.connect(
                        database=target['database'],
                        user=target['user'],
                        password=target['password'],
                        host=target['host'],
                        port=target['port']
                )
                cur = conn.cursor()

                df2 = pd.DataFrame(data_set, index=None, columns=None)
                print(df2.values)
                
                for tups in tuple(df2.values.tolist()):

                        print(tuple(tups))

                        sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                        cur.execute(sql2)
                        conn.commit()

        except (Exception, ps.DatabaseError) as error:
                print(error)

        return df2


def source_mysql(sources):
        try:

                conn = msql.connect(host=sources['host'],
                                        user=sources['user'],
                                        password=sources['password'],
                                        port=sources['port'],
                                        database=sources['database']
                                        )
                cur = conn.cursor()

                sql = f"select * from {sources['source_table_name']}"
                cur.execute(sql)
                df = pd.DataFrame(cur)
                print('read the table', df)
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def target_mysql(target,data_set):
        try:

                conn = msql.connect(host=target['host'],
                                        user=target['user'],
                                        password=target['password'],
                                        port=target['port'],
                                        database=target['database']
                                        )
                cur = conn.cursor()

                df2 = pd.DataFrame(data_set, index=None, columns=None)
                
                for tups in tuple(df2.values.tolist()):

                        sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                        cur.execute(sql2)
                        conn.commit()
        except (Exception, ps.DatabaseError) as error:
                print(error)

        return df2

def source_sql_lite(sources):
        try:

                # create con object to connect
                # the database geeks_db.db
                conn = sqlite3.connect("mydb.db")
                cur = conn.cursor()


                sql = f"SELECT * from {sources['source_table_name']}"
                cur.execute(sql)
                df = pd.DataFrame(cur)
                print('read the table', df)
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def target_sql_lite(data_set,target):
        try:


                conn = sqlite3.connect("mydb.db")
                cur = conn.cursor()
                df2 = pd.DataFrame(data_set, index=None, columns=None)
                
                for tups in tuple(df2.values.tolist()):

                        sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                cur.execute(sql2)
                conn.commit()
        except (Exception, ps.DatabaseError) as error:
                print(error)
        
        return df2

def source_oracle(sources):
        try:

                connstr =f"{sources['user']}/{sources['password']}@{sources['host']}:{sources['port']}/{sources['database']}"

                conn = cx_Oracle.connect(connstr)
                cur = conn.cursor()
                print("connected")


                sql = f"select * from {sources['source_table_name']}"
                cur.execute(sql)
                df = pd.DataFrame(cur)
                print('read the table', df)
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def target_oracle(data_set,target):
        try:

                connstr = f"{target['user']}/{target['password']}@{target['host']}:{target['port']}/{target['database']}"

                conn = cx_Oracle.connect(connstr)
                cur = conn.cursor()
                print("connected")
                df2 = pd.DataFrame(data_set, index=None, columns=None)
                print(df2.values)
                
                for tups in tuple(df2.values.tolist()):
                        print(tuple(tups))

                        sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                cur.execute(sql2)
                conn.commit()
        except (Exception, ps.DatabaseError) as error:
                print(error)
        
        return df2





def source_sf(sources):
        print(sources)
        try:

                # import pdb
                # pdb.set_trace()
                print('Connecting to te sf database...')
                conn = snowflake.connector.connect(
                user=sources['user'],
                password=sources['password'],
                account=sources['account'],
                warehouse=sources['warehouse'],
                database=sources['database'],
                schema=sources['schema'],
                role='ACCOUNTADMIN',
                insecure_mode=True,
                )
                print(sources)
                
                print("Connection successful")
                cur=conn.cursor()
                sql=f"select * from {sources['source_table_name']}"
                df = pd.read_sql_query(sql,conn)
                print(df)
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df




def target_sf(data_set,target):
        # import pdb
        # pdb.set_trace()
        try:

                url = snowflake.connector.connect(
                        user=target['user'],
                        password=target['password'],
                        account=target['account'],
                        warehouse=target['warehouse'],
                        database=target['database'],
                        schema=target['schema'],
                        role='ACCOUNTADMIN'
                )
                
                cur = url.cursor()
                # import pdb
                # pdb.set_trace()
                print("sf connected")
                df2 = pd.DataFrame(data_set, index=None, columns=None)

                for tups in tuple(df2.values.tolist()):

                        sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                        cur.execute(sql2)
                        
        except(Exception, ps.DatabaseError) as error:
                print(error)



def source_teradata(sources):
        try:

                url = teradatasql.connect(
                        host=sources['server'],
                        user=sources['username'],
                        password=sources['password'],
                        encryptdata='true',
                )
                cur = url.cursor()
                print("teradata connected")
                sql = f"select * from {sources['source_table_name']}"
                cur.execute(sql)
                df = pd.DataFrame(cur)
                print(df)
                print('read done')
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def target_teradata(data_set,target):
        try:

                url = teradatasql.connect(
                        host=target['server'],
                        user=target['username'],
                        password=target['password'],
                        encryptdata='true',
                )
                cur = url.cursor()
                print("teradata connected")
                df2 = pd.DataFrame(data_set, index=None, columns=None)
                print(df2.values)

                
                for tups in tuple(df2.values.tolist()):
                        print(tuple(tups))

                        sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                        cur.execute(sql2)
                        url.commit()
        except (Exception, ps.DatabaseError) as error:
                print(error)

def source_sqlserver(sources):
        try:
                print('Connecting to the PostgresSQL database...')
                conn = pyodbc.connect(DRIVER=sources['driver'],
                                SERVER=sources['server'],
                                PORT=sources['port'],
                                DATABASE=sources['database'],
                                UID=sources['username'],
                                PWD=sources['password'])
                cur = conn.cursor()
                print("Connection successful")
                sql = f"select * from {sources['source_table_name']}"
                df = pd.read_sql_query(sql, conn)
                print(df)
                print(type(df))
        except (Exception, ps.DatabaseError) as error:
                print(error)
                id=sources['run_id']
                put(run_ids=id)
        return df

def target_sqlserver(target,data_set):
        try:
                # import pdb
                # pdb.set_trace()
                print('Connecting to the PostgresSQL database...')
                url = pyodbc.connect(DRIVER=target['driver'],
                                SERVER=target['server'],
                                PORT=target['port'],
                                DATABASE=target['database'],
                                UID=target['username'],
                                PWD=target['password'])
                print("Connection successful")

                cur = url.cursor()
                df2 = pd.DataFrame(data_set, index=None, columns=None)

                for tups in tuple(df2.values.tolist()):
                                print(tuple(tups))

                                sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                                cur.execute(sql2)
                                url.commit()
                # Closing the connection
        except (Exception, pyodbc.DatabaseError) as error:
                print(error)
                id=target['run_id']
                put(run_ids=id)

def getColumnDtypes(dataTypes):
#     import pdb
#     pdb.set_trace()
                        
    dataList = []
    for x in dataTypes:
        if(x == 'integer'):
            dataList.append('int')
        elif (x == 'float64'):
            dataList.append('float')
        elif (x == 'bool'):
            dataList.append('boolean')
        else:
            dataList.append('varchar')
    return dataList
    
def create_tbl(i):
        # import pdb
        # pdb.set_trace()
        data_ty=[]  
        column=[]
        for s in i:
                column.append(s['column_name'])
                data_ty.append(s['data_type'])
        # print(column['column_name'])
        # print(column['data_type'])
        print('hi')
        print('hlo')
        dty=getColumnDtypes(data_ty)
        set=[]
        for i ,j in zip(column,dty):
                print(i+' '+j+',')
                dummy=i+' '+j
                set.append(dummy)
        return set

def getColumnDtypes_sqlserver(dataTypes):
#     import pdb
#     pdb.set_trace()
                        
    dataList = []
    for x in dataTypes:
        if (x == 'bigint'):
            dataList.append('int')
        elif (x == 'float(255)'):
            dataList.append('float(255)')
        elif (x == 'bool'):
            dataList.append('boolean')
        else:
            dataList.append('varchar(500)')
    return dataList
    
def create_tbl_sqlserver(i):
        # import pdb
        # pdb.set_trace()
        data_ty=[]  
        column=[]
        for s in i:
                column.append(s['column_name'])
                data_ty.append(s['data_type'])
        # print(column['column_name'])
        # print(column['data_type'])
        print('hi')
        print('hlo')
        dty=getColumnDtypes(data_ty)
        set=[]
        for i ,j in zip(column,dty):
                print(i+' '+j+',')
                dummy=i+' '+j
                set.append(dummy)
        return set
# def getColumnDtypes(dataTypes):
#     import pdb
#     pdb.set_trace()
                        
#     dataList = []
#     for x in dataTypes:
#         if(x == 'int64'):
#             dataList.append('int')
#         elif (x == 'float64'):
#             dataList.append('float')
#         elif (x == 'bool'):
#             dataList.append('boolean')
#         else:
#             dataList.append('varchar')
#     return dataList
    
# def create_tbl(column,dtype):
#     import pdb
#     pdb.set_trace()  
#     dty=getColumnDtypes(dataTypes=dtype)
#     set=[]
#     for i ,j in zip(column,dty):
#         print(i+' '+j+',')
#         dummy=i+' '+j
#         set.append(dummy)
#     return set