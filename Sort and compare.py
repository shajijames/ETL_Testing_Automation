import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 100)

sample1=pd.read_csv(r'C:\Users\sselvaku\OneDrive - Systech Solutions, Inc\Dev\Sample_1.txt', sep='|')
sample2=pd.read_csv(r'C:\Users\sselvaku\OneDrive - Systech Solutions, Inc\Dev\Sample_2.txt', sep='|')
#sample2=pd.read_csv(r'C:\Users\sselvaku\OneDrive - Systech Solutions, Inc\Dev\Sample_2_mismatch.txt', sep='|')


sample1.head()
sample2.head()

col1=sample1.columns
col2=sample2.columns
cols=list(set(col1.sort_values()).intersection(set(col2.sort_values())))
print('Common column(s): '+', '.join(cols))

sample1.sort_values(['Country', 'Region', 'Order Date'], axis=0, ascending=True, inplace=True)
sample2.sort_values(['Country', 'Region', 'Order Date'], axis=0, ascending=True, inplace=True)


data1=sample1[cols].reset_index(drop=True)
data2=sample2[cols].reset_index(drop=True)


data1.head()
data2.head()

result=data1==data2
bool_list=[]
mis_cols=[]
mis_index=[]
for i in cols:
    if all(result[i])==True:
        bool_list.append(True)
    else:
        bool_list.append(False)
        mis_cols.append(i)
        for j in range(len(result[i])):
            if result[i][j]==False:
                mis_index.append(j)               
un_df=pd.concat([data1.iloc[list(set(mis_index))],data2.iloc[list(set(mis_index))]],axis=1)

from datetime import datetime
time=datetime.now()
file='rec'+'_'+time.strftime("%j")+time.strftime("%Y")+time.strftime("%H")+time.strftime("%M")+time.strftime("%S")+'.csv'
file_path='C:\\Users\\sselvaku\\OneDrive - Systech Solutions, Inc\Dev\\'+file
  
if all(bool_list)==True:
    print('Records are matching')
else:
    print(str(len(set(mis_index)))+' records unmatched')
    print('Column(s): '+', '.join(mis_cols))
    out=input('Do you want the unmatched records to be stored locally (y/n): ')
    if out=='y' or out=='Y':
        un_df.to_csv(file_path)
        print('File saved successfully '+file_path)
    else:
        print('')

