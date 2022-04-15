import threading
import sqlite3
from sqlite3 import Error
import numpy as np
import os 
import pandas as pd
from queue import Queue
import time

def multiprocess_pipeline(db_file_name,db_new_file_name):
    min_value = 99999
    


    # Split File Function
    def create_files(db, no_of_records_per_file):
        try: 
            con = sqlite3.connect(db)
            curr = con.cursor()
            data = curr.execute("SELECT * from employee;")
            column_names =[elem[0] for elem in data.description]
            array = np.array(curr.fetchall())
            i = 0 
            j = 1
            while i < len(array):
                data = array[i: i + no_of_records_per_file]
                df = pd.DataFrame(data)
                df.columns = column_names
                df.to_csv(f'./file_split/chor{j}.csv',index = False)
                j+=1
                i+= no_of_records_per_file

        except Exception as e:
            print(e)


    # E T L 
    def perform_transformation(file_name):
        try :
            df = pd.read_csv(file_name)
            for index,elem in df.iterrows():
                    df.loc[index,'name'] = df.loc[index,'name'].upper()
                    df.loc[index, 'age'] = df.loc[index, 'age'] + 1
                    df.loc[index, 'salary'] = (102/100)*df.loc[index, 'salary']
                    df.loc[index, 'address'] = df.loc[index, 'address'].upper()
                    df.loc[index,'gender'] = df.loc[index,'gender'].upper()
            return df
            # df.to_sql('employee', conn, if_exists='append', index=False)
            # print(f"Done {file_name}")
        except Exception as e:
            print(e) 
            
    ## Producer will create and Transform data and Store it in Queue
    def create_work(work):
        for file in os.listdir('file_split'):
            work.put(perform_transformation(f'./file_split/{file}'))

    ## Consumer will put data back in Database
    def perform_work(work,consumed):
        while consumed != 0: 
            if not work.empty():
                v = work.get()
                v.to_sql('employee', conn, if_exists='append', index=False)
                consumed -= 1

    # Perform Splits on Each File
    for i in range(1,11):
        try:
            conn = sqlite3.connect(db_new_file_name,check_same_thread=False)
        except Exception as e:
            print(e)

        create_files(db_file_name,int((i*10/100)*100))
        

        work = Queue()
        consumed = len(os.listdir('./file_split')) 
        producer = threading.Thread(target = create_work,args=[work],daemon=True)
        consumer = threading.Thread(target = perform_work,args=[work,consumed],daemon = True)

        start = time.time()
        producer.start()
        consumer.start()

        producer.join()
        consumer.join()

        end = time.time()
        for elem in os.listdir('file_split'):
            os.remove(f'./file_split/{elem}')
        conn.close()
        os.remove(db_new_file_name)

        min_value = min(min_value, end - start)
    print(min_value,db_file_name)

    return min_value




    

