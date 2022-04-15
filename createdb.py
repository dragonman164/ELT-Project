import sqlite3
from sqlite3 import Error

def create_db(db_file,number_of_records):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        cur = conn.cursor()
        cur.executescript("""
        CREATE TABLE employee(
        ID integer PRIMARY KEY AUTOINCREMENT,
        name text NOT NULL,
        age integer NOT NULL,
        salary integer NOT NULL,
        address text NOT NULL,
        gender text NOT NULL,
        mobileNo integer NOT NULL
        );
        """)

        for i in range(number_of_records//5):
            cur.executescript("""
            INSERT INTO employee (name,age,salary,address,gender,mobileNo)
            VALUES('Paras', 21,100000,"#135 old bishan nagar patiala","male",8360386290);
            
            INSERT INTO employee (name,age,salary,address,gender,mobileNo)
            VALUES('sanidhiya', 25,1000000,"#1366  rohini delhi","male",8754674737);
            
            INSERT INTO employee (name,age,salary,address,gender,mobileNo)
            VALUES('yoo yep yoo', 21,450000,"#1988 bishan nagar patiala","male",8360999290);

            INSERT INTO employee (name,age,salary,address,gender,mobileNo)
            VALUES('Infernape', 21,10000,"#1357 old nagar patiala","male",838789290);
            
            INSERT INTO employee (name,age,salary,address,gender,mobileNo)
            VALUES('pikachu', 5,100099,"#13578 vintage ambala","male",838789290);
            """)
        conn.close()
    except Error as e:
        print(e)
