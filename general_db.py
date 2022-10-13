import mysql.connector
import db_config as c



def get_db():
    mydb = mysql.connector.connect(
        host=c.config['host'],
        user=c.config['user'],
        password=c.config['password']
    )
    return mydb

def make_query(query):
    mydb = get_db()

    with mydb as mycursor:
        mycursor.excecute(query)


def create_db(db_name):
    query = "CREATE DATABASE "+db_name
    make_query(query)


def show_db():
    mydb = get_db()
    mycursor = mydb.cursor()    
    query = "SHOW DATABASES"
    mycursor.execute(query)

    for x in mycursor:
        print(x)
    
    mycursor.close()
    mydb.close()


def connect_to_db(db_name):

    mydb = mysql.connector.connect( 
        host=c.config['host'],
        user=c.config['user'],
        password=c.config['password'],
        database=db_name
    )

    return mydb


def create_table(db_name, table_name, col_dict):
    """
    col_dict = {
        col1: {col_name: name, col_type: type},
        col2: {col_name: name, col_type: type},
        col3: {col_name: name, col_type: type},
        ...
        coln: {col_name: name, col_type: type},
    }
    """
    my_str = ""

    for k,v in col_dict.items():
        my_str += (v['col_name'] + v['col_type']+",")

    query = "CREATE TABLE "+table_name+" (id INT AUTO_INCREMENT PRIMARY KEY, "+ my_str[:-1] + ")"
    
    mydb = connect_to_db(db_name)
    mycursor = mydb.cursor()
    mycursor.execute(query)
    mycursor.close()
    mydb.close()




def show_tables(db_name):
    mydb = connect_to_db(db_name)

    mycursor = mydb.cursor()
    mycursor.execute("SHOW TABLES")

    for x in mycursor:
        print(x)

    mycursor.close()
    mydb.close()


def insert_into_table(table_names, column_names, values):

    mydb = connect_to_db(db_name)
    mycursor = mydb.cursor()

    col_str = ""

    n_col = len(column_names)
    for idx, col in enumerate(column_names):
        col_str += col
        if idx != n_col-1:
            col_str += ", "

    
    n_val = len(values)
    val_str = ""
    for i in range(n_val):
        val_str += "%s"
        if i!=n_val-1:
            val_str +=","

    sql = "INSERT INTO "+ table_name +"("+col_str+") VALUES ("+val_str+")"
    val = values
    mycursor.execute(sql, val)

    mydb.commit()

    print(mycursor.rowcount, "record inserted.")

    # mycursor.execute("CREATE TABLE customers (name VARCHAR(255), address VARCHAR(255))")

# my_col = {
#     'col1': {'col_name': 'name' , 'col_type': 'VARCHAR(255)' },
#     'col2': {'col_name':  'address', 'col_type': 'VARCHAR(255)' }        
# }

