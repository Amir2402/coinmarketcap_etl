import cx_Oracle
import pandas as pd
from dotenv import load_dotenv 
import os 
from glob import glob 
def load_data() : 
    my_file = '/home/amir/firstproj/extracted_data.csv'
    df = pd.read_csv(my_file)
    load_dotenv()
    #THIS DATABASE IS RUNNING ON A DOCKER CONTAINER
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    db_host = '172.17.0.2'
    db_port = '1521' 
    connection_cred = f"{db_user}/{db_pass}@{db_host}:{db_port}" 
    conn = cx_Oracle.connect(connection_cred)
    curs = conn.cursor()
    #VERIFYING THAT THE TABLE IS NOT EXISTING
    curs.execute('SELECT count(*) FROM all_tables WHERE table_name = \'CRYPTO\'')
    if curs.fetchall()[0][0] == 2 :
        curs.execute('DROP TABLE CRYPTO')
        
    create_statement = '''CREATE TABLE CRYPTO(
                        ID VARCHAR(20) PRIMARY KEY , 
                        NAME VARCHAR(50) NOT NULL , 
                        SYMBOL VARCHAR(10) NOT NULL , 
                        PRICE REAL NOT NULL )  
                        '''
    curs.execute(create_statement)

    for itr in range(0 , df.shape[0]) : 
        insert_statement = f'''INSERT INTO CRYPTO VALUES(
        '{df.iloc[itr , 0]}',
        '{df.iloc[itr , 1]}' ,
        '{df.iloc[itr , 2]}' , 
        '{df.iloc[itr , 3]}' 
        ) 
        '''
        curs.execute(insert_statement)
    conn.commit()
if __name__ == '__main__' : 
    load_data()
    