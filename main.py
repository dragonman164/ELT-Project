import os 
import pickle
from case1 import ELT_row_by_row
from case2 import file_to_db
from case3 import multiprocess_pipeline
from createdb import create_db
import time
from matplotlib import pyplot as plt
import numpy as np


os.makedirs('Databases/old_databases')
os.makedirs('Databases/new_databases')
os.makedirs('Databases/files')
f = open("case1.pickle","wb")
g = open("case2.pickle","wb")
h = open("case3.pickle","wb")
data1,data2,data3 = {},{},{}
for i in range(1,11):
    create_db(f"./Databases/old_databases/database{i}.db",100*i)


## Create Dictionary for Case1 : 
for i in range(1,11):
    start = time.time()
    ELT_row_by_row(f'./Databases/old_databases/database{i}.db',f'./Databases/new_databases/database{i}.db')
    end = time.time()
    data1[str(i*100) + ' Records'] = end - start 

pickle.dump(data1,f)

for file in os.listdir('./Databases/new_databases'):
    os.remove(f'./Databases/new_databases/{file}')

## Create Dictionary for Case2:
for i in range(1,11):
    start = time.time()
    file_to_db(f'./Databases/old_databases/database{i}.db',f'./Databases/new_databases/database{i}.db',f'./Databases/files/file{i}.csv')
    end = time.time()
    data2[str(i*100) + ' Records'] = end - start

pickle.dump(data2,g)

for file in os.listdir('./Databases/new_databases'):
    os.remove(f'./Databases/new_databases/{file}')

for i in range(1,11):
    check = multiprocess_pipeline(f'./Databases/old_databases/database{i}.db',f'./Databases/new_databases/database{i}.db')
    data3[str(i*100) + ' Records'] = check

pickle.dump(data3,h)
f.close()
g.close()
h.close()
f = open("case1.pickle","rb")
g = open("case2.pickle","rb")
h = open("case3.pickle","rb")
data1 = pickle.load(f)
data2 = pickle.load(g)
data3 = pickle.load(h)

x = np.arange(1, 11, 1)

plt.plot(data1.values(),label="Case 1")
plt.plot(data2.values(),label = "Case 2")
plt.plot(data3.values(),label= "Case 3")
plt.xticks(x)
plt.xlabel("No. of Records (per 1000)")
plt.ylabel("Time Taken in Seconds")
plt.legend()
plt.savefig('result.png')