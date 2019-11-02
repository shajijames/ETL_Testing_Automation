# -*- coding: utf-8 -*-
"""
Created on Fri Jul 12 15:46:43 2019
@author: Shaji James

"""

# =================================================================================
# This script is used to perform column level check between SQL Server and MySQL
# =================================================================================


import pandas as pd
from datetime import datetime
import pyodbc

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

def CLC(table,mssql_engine,mysql_engine):
    
    global execution_status
    
    systime=datetime.now()
    
    start_time=systime.strftime("%Y")+'-'+systime.strftime("%m")+'-'+systime.strftime("%d")+' '+systime.strftime("%H")+':'+systime.strftime("%M")+':'+systime.strftime("%S")
    
    test_id='RRC'+systime.strftime("%Y")+systime.strftime("%m")+systime.strftime("%d")+systime.strftime("%H")+systime.strftime("%M")+systime.strftime("%S")+systime.strftime("%f")
    
    log_path='C:\\RRC\\LOG\\LOG_'+table+'_'+test_id+'.txt'
    
    mismatch_path='C:\\RRC\\MISMATCH\\MISMATCH_'+table+'_'+test_id+'.csv'
    
    execution_status='RUNNING'
    
    log_list=[]
    
    log_list.append('Test ID: '+test_id)
    
    log_list.append('Table: '+table)
    
    log_list.append('START TIME: '+start_time)
    
    log_list.append(str(datetime.now())+': INSERTING TEST DETAILS TO LOG TABLE')
    
    try:     
        insert_log_query="INSERT INTO Traget_Shaji.dbo.ETL_TESTING_LOG_TABLE (Test_Id, Table_Name, Start_Time, Execution_Status) VALUES ("+"'"+test_id+"','"+table+"','"+start_time+"','"+execution_status+"')"        
        mssql_engine.execute(insert_log_query)
    except Exception as e:
        log_list.append('Failed while INSERTING TEST DETAILS TO LOG TABLE: '+str(e))
        execution_status='FAILED'
        
    log_list.append(str(datetime.now())+': GETTING THE SQL & mysql QUERIES, PRIMARY KEYS')
        
    if execution_status!='FAILED':
        try:
            mssql_query=mssql_query_dict[table]
            log_list.append('MSSQL QUERY: '+mssql_query)
            mysql_query=mysql_query_dict[table]
            log_list.append('MySQL QUERY: '+mysql_query)
            keys=key_dict[table]
            log_list.append('KEYS: '+keys)
            key_list=key_dict[table].split(',')
        except Exception as e:
            log_list.append('Failed while GETTING THE SQL & mysql QUERIES, PRIMARY KEYS: '+str(e))
            execution_status='FAILED'
        
    log_list.append(str(datetime.now())+': READING THE DATA FROM SQL SERVER')
    
    if execution_status!='FAILED':
        try:
            src=pd.read_sql(mssql_query,mssql_engine)
        except Exception as e:
            log_list.append('Failed while READING THE DATA FROM SQL SERVER: '+str(e))
            execution_status='FAILED'

    log_list.append(str(datetime.now())+': READING THE DATA FROM mysql')
    
    if execution_status!='FAILED':
        try:
            tgt=pd.read_sql(mysql_query,mysql_engine)
        except Exception as e:
            log_list.append('Failed while READING THE DATA FROM mysql: '+str(e))
            execution_status='FAILED'

    log_list.append(str(datetime.now())+': DIFFERENTIATING SOURCE AND TARGET COLUMNS')
    if execution_status!='FAILED':
        try:
            src_k=[]
            src_columns=[]
            for i  in src.columns:
                if str.lower(i) in [str.lower(key) for key in key_list]:
                    src_columns.append(str.lower(i))
                    src_k.append(str.lower(i))
                else:
                    src_columns.append(str(i) + '_src')           
            src.columns = src_columns
            tgt_k=[]
            tgt_columns=[]
            for i  in tgt.columns:
                if str.lower(i) in [str.lower(key) for key in key_list]:
                    tgt_columns.append(str.lower(i))
                    tgt_k.append(str.lower(i))
                else:
                    tgt_columns.append(str(i) + '_tgt')           
            tgt.columns = tgt_columns
        except Exception as e:
            log_list.append('Failed while DIFFERENTIATING SOURCE AND TARGET COLUMNS: '+str(e))
            execution_status='FAILED'
    
    
    log_list.append(str(datetime.now())+': CHECKING IF THE GROUP BY MAKES THE RECORD LEVEL SAME AS ACTUAL')
    if execution_status!='FAILED':
        try: 
            index_unique_flag=[]
            
            if src.groupby(src_k).count().shape[0]==src.shape[0]:
                index_unique_flag.append(True)
            else:
                index_unique_flag.append(False)
                
            if tgt.groupby(tgt_k).count().shape[0]==tgt.shape[0]:
                index_unique_flag.append(True)
            else:
                index_unique_flag.append(False)
        except Exception as e:
            log_list.append('Failed while CHECKING IF THE GROUP BY MAKES THE RECORD LEVEL SAME AS ACTUAL: '+str(e))
            execution_status='FAILED'            


    if execution_status!='FAILED':
        try: 
            if all(index_unique_flag)==True:
                log_list.append(str(datetime.now())+': JOINING THE TABLES')
                try:
                    df=tgt.set_index(tgt_k).join(src.set_index(src_k),how='left')
                except Exception as e:
                    log_list.append('Failed while JOINING THE TABLES: '+str(e))
                    execution_status='FAILED'
                
                log_list.append(str(datetime.now())+': FINDING THE TARGET COLUMN AND SOURCE COLUMN TO BE COMPARED')
                if execution_status!='FAILED':
                    try:
                        ma_list=[]    
                        for i in range(len(df.columns)):
                            if df.columns[i][-3:]=='tgt':
                                for j in range(len(df.columns)):
                                    if df.columns[j][-3:]=='src':
                                        if str.lower(df.columns[i][:-4])==str.lower(df.columns[j][:-4]):
                                            ma_list.append([j,i])                        
                        match_cols=''
                        for i in range(len(ma_list)):
                            match_cols+=str(i+1)+': '+df.columns[ma_list[i][1]]+' = '+df.columns[ma_list[i][0]]+' , '
                        log_list.append('Matching columns '+match_cols)
                    except Exception as e:
                        log_list.append('Failed while FINDING THE TARGET COLUMN AND SOURCE COLUMN TO BE COMPARED: '+str(e))
                        execution_status='FAILED'
                    
                log_list.append(str(datetime.now())+': COMPARISION STARTED')
                if execution_status!='FAILED':
                    try:
                        mis_cols=[]
                        res=[]
                        index=[]
                        for i in range(len(ma_list)):
                            if all(df[df.columns[ma_list[i][0]]].apply(lambda x:str(x).strip()).astype(str).fillna(str(0))==df[df.columns[ma_list[i][1]]].apply(lambda x:str(x).strip()).astype(str).fillna(str(0)))==True:
                                res.append(True)
                               
                            else:
                                res.append(False)
                                mis_cols.append(df.columns[ma_list[i][0]])
                                mis_cols.append(df.columns[ma_list[i][1]])
                                for j in range(len(df[df.columns[ma_list[i][0]]].apply(lambda x:str(x).strip()).astype(str).fillna(str(0))==df[df.columns[ma_list[i][1]]].apply(lambda x:str(x).strip()).astype(str).fillna(str(0)))):
                                    if list(df[df.columns[ma_list[i][0]]].apply(lambda x:str(x).strip()).astype(str).fillna(str(0))==df[df.columns[ma_list[i][1]]].apply(lambda x:str(x).strip()).astype(str).fillna(str(0)))[j]==False:
                                        index.append(j)
                        un_df=df[mis_cols].iloc[list(set(index))]
                    except Exception as e:
                        log_list.append('Failed while COMPARING: '+str(e))
                        execution_status='FAILED'
                            
                log_list.append(str(datetime.now())+': TEST RESULT:')
                if execution_status!='FAILED':
                    endtime=datetime.now()
                    end_time=endtime.strftime("%Y")+'-'+endtime.strftime("%m")+'-'+endtime.strftime("%d")+' '+endtime.strftime("%H")+':'+endtime.strftime("%M")+':'+endtime.strftime("%S")
                    try:
                        if all(res)==True:
                            mismatch_count=0
                            execution_status='SUCCESS'
                            log_list.append('COLUMN LEVEL CHECK PASSED')
                            result='PASS'
                            log_list.append(str(datetime.now())+': UPDATING LOGS')
                            update_log_query="update Traget_Shaji.dbo.ETL_TESTING_LOG_TABLE set End_Time ='"+end_time+"',"+"Test_Result='"+result+"',"+"Execution_Status='"+execution_status+"',"+"Mismatch_Count="+str(mismatch_count)+","+"Log_Path='"+log_path.replace('\\','/')+"' where Test_id='"+test_id+"'"
                            try:
                                mssql_engine.execute(update_log_query)
                            except Exception as e:
                                log_list.append('Failed while UPDATING LOGS: '+str(e))
                                execution_status='FAILED'                              
                        else:
                            log_list.append((str(len(set(index)))+' records unmatched'))
                            result='FAIL'
                            log_list.append('Column level check Failed')
                            un_df.to_csv(mismatch_path)
                            log_list.append('File saved successfully '+mismatch_path)
                            mismatch_count=str(len(set(index)))
                            execution_status='SUCCESS'
                            log_list.append(str(datetime.now())+': UPDATING LOGS')
                            update_log_query="update Traget_Shaji.dbo.ETL_TESTING_LOG_TABLE set End_Time ='"+end_time+"',"+"Test_Result='"+result+"',"+"Execution_Status='"+execution_status+"',"+"Mismatch_Count="+str(mismatch_count)+","+"Mismatch_Path='"+mismatch_path.replace('\\','/')+"',"+"Log_Path='"+log_path.replace('\\','/')+"' where Test_id='"+test_id+"'"
                            try:
                                mssql_engine.execute(update_log_query)
                            except Exception as e:
                                log_list.append('Failed while UPDATING LOGS: '+str(e))
                                execution_status='FAILED' 
                    except Exception as e:
                        log_list.append('Failed while getting the TEST RESULT: '+str(e))
                        execution_status='FAILED'
            else:
                log_list.append('The records grouped at the level of key columns are not unique')
                log_list.append(str(datetime.now())+': UPDATING LOGS')
                update_log_query="update Traget_Shaji.dbo.ETL_TESTING_LOG_TABLE set End_Time ='"+end_time+"',"+"Log_Path='"+log_path.replace('\\','/')+"',"+"Execution_Status='STOPPED' where Test_id='"+test_id+"'"
                try:
                    mssql_engine.execute(update_log_query)
                except Exception as e:
                    log_list.append('Failed while UPDATING LOGS: '+str(e))
                    execution_status='FAILED'          
        except Exception as e:
            log_list.append('Failed while CHECKING IF THE GROUP BY MAKES THE RECORD LEVEL SAME AS ACTUAL: '+str(e))
            execution_status='FAILED'
        
    endtime=datetime.now()
    end_time=endtime.strftime("%Y")+'-'+endtime.strftime("%m")+'-'+endtime.strftime("%d")+' '+endtime.strftime("%H")+':'+endtime.strftime("%M")+':'+endtime.strftime("%S")

    if execution_status=='FAILED':
        log_list.append(str(datetime.now())+': UPDATING LOGS')
        update_log_query="update Traget_Shaji.dbo.ETL_TESTING_LOG_TABLE set End_Time ='"+end_time+"',"+"Execution_Status='"+execution_status+"',"+"Log_Path='"+log_path.replace('\\','/')+"' where Test_id='"+test_id+"'"
        try:
            mssql_engine.execute(update_log_query)
        except Exception as e:
            log_list.append('Failed while UPDATING LOGS: '+str(e))
            execution_status='FAILED' 
        
    try:   
        file = open(log_path,'a')
        for element in log_list:
            file.write(str(element)+'\n')
        file.close()
        print(table+': Log stored in '+log_path)
    except Exception as e:
        print(table+': Log updation failed'+str(e))




mssql_engine=pyodbc.connect(r'DSN=MSSQL_DSN;'
                'Authentication=ActiveDirectoryPassword')

mysql_engine=pyodbc.connect(r'DSN=MySQL_DSN')

meta_table=pd.read_sql("select * from Traget_Shaji.dbo.ETL_TESTING_META_TABLE where TestFlag='TRUE';",mssql_engine)

key_dict=dict()
for i in range(meta_table.shape[0]):
    key_dict[meta_table['TargetTable'][i]]=meta_table['PrimaryKeys'][i]

mssql_query_dict = dict()
for i in range(meta_table.shape[0]):
    source_extract=meta_table['SourceExtract'][i].replace('&quote&',"'").replace(';','')
    if source_extract[0:7]=='select ':
        mssql='select * from ('+source_extract.strip()+')src order by '+meta_table['PrimaryKeys'][i]
    else:
        mssql='select * from '+meta_table['SourceDatabase'][i].strip()+'.'+meta_table['SourceSchema'][i].strip()+'.'+source_extract.strip()+'  order by '+meta_table['PrimaryKeys'][i]
    mssql_query_dict[meta_table['TargetTable'][i]]=mssql

mysql_query_dict = dict()
for i in range(meta_table.shape[0]):
    mysql='select * from '+meta_table['TargetDatabase'][i].strip()+'.'+meta_table['TargetTable'][i].strip()+'  order by '+meta_table['PrimaryKeys'][i]
    mysql_query_dict[meta_table['TargetTable'][i]]=mysql

        
    
###########################################################
    
#TEST FOR ALL TABLES

for i in meta_table['TargetTable']:
    table_name=i
    try:
        CLC(table_name,mssql_engine,mysql_engine)
        print(table_name+' : COMPLETED')
    except Exception as e:
        print(table_name+ ': '+str(e))
    mssql_engine.commit()
    
        
###########################################################

mssql_engine.commit()
mysql_engine.commit()
mssql_engine.close()
mysql_engine.close()