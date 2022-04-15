import sqlite3 
from sqlite3 import Error


## Case 1:
def ELT_row_by_row(db_file,db_file_new):
    conn = None
    try:
        conn1 = sqlite3.connect(db_file)
        curr1 = conn1.cursor()

        conn2 = sqlite3.connect(db_file_new)
        curr2 = conn2.cursor()

        curr2.executescript("""
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
        sqlite_select_query = """SELECT * from employee"""
        curr1.execute(sqlite_select_query)
        records = curr1.fetchall()

        for row in records:
            curr2.executescript(
                f"""
                  INSERT INTO employee (name,age,salary,address,gender,mobileNo)
            VALUES('{row[1].upper()}', {row[2] + 1},{(102/100)*row[3]},'{row[4].upper()}','{row[5].upper()}',{row[6]});
                """
            )
        curr1.close()
        curr2.close()
        conn1.close()
        conn2.close()
    except Error as e:
        print(e)