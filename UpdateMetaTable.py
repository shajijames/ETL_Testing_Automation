# -*- coding: utf-8 -*-
"""
Created on Tue Oct 29 11:19:58 2019

@author: SSelvaku
"""


import pyodbc

def insert_meta():
    src_database=  input('Enter source database: ')
    src_schema=  input('Enter source schema: ')
    src_extract = input('Enter source table/query: ')
    tgt_database=  input('Enter target database: ')
    tgt_schema=  input('Enter target schema: ')
    tgt_table = input('Enter target table/query: ')
    primary_keys = input('Enter primary keys (Comma Separated): ')
    grouping_column = input('Enter grouping column (High level of granularity): ')
    
    
    mssql_con=pyodbc.connect(r'DSN=MSSQL_DSN;'
                    'Authentication=ActiveDirectoryPassword')
    
    import re
    src=re.sub(r"'", r"&quote&", src_extract)
    
    meta_insert="INSERT INTO Traget_Shaji.dbo.ETL_TESTING_META_TABLE (TargetDatabase ,TargetSchema ,TargetTable ,SourceDatabase ,SourceSchema ,SourceExtract ,PrimaryKeys ,GroupingColumn) VALUES ('"+tgt_database+"','"+tgt_schema+"','"+tgt_table+"','"+src_database+"','"+src_schema+"','"+src+"','"+primary_keys+"','"+grouping_column+"');"
    
    mssql_con.execute(meta_insert)
    mssql_con.commit()
    mssql_con.close()

insert_meta()
