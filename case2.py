import sqlite3
import pandas as pd

## Case 2: 
def file_to_db(db_file,new_db_file,file_name):
    conn = None
    try:
        conn1 = sqlite3.connect(db_file)
        curr1 = conn1.cursor()
        conn2 = sqlite3.connect(new_db_file)
        sqlite_select_query = """SELECT * from employee"""
        data = curr1.execute(sqlite_select_query)
        column_names =[elem[0] for elem in data.description]
        df = pd.DataFrame(curr1.fetchall())
        df.columns = column_names
        df.to_csv(file_name,index = False)
        
        
        df = pd.read_csv(file_name)
        for index,elem in df.iterrows():
            df.loc[index,'name'] = df.loc[index,'name'].upper()
            df.loc[index, 'age'] = df.loc[index, 'age'] + 1
            df.loc[index, 'salary'] = (102/100)*df.loc[index, 'salary']
            df.loc[index, 'address'] = df.loc[index, 'address'].upper()
            df.loc[index,'gender'] = df.loc[index,'gender'].upper()
        df.to_sql('employee', conn2, if_exists='replace', index=False)
        conn2.close()
        conn1.close()
    except Exception as e:
        print(e)
