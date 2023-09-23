import mysql.connector
from data_visualization import df
from sqlalchemy import create_engine

def create_db_connection():
    db_params = {
        'host': 'localhost',
        'user': 'root',
        'password': 'new_password'
    }
    # 'password': '@Ak_mb#89621997'
    #establish connection
    conn = mysql.connector.connect(**db_params)

    #create a cursor
    cursor = conn.cursor()
    print('MYSQL server connection successful')

    try:
        cursor.execute('USE data_visual_proj')
    except:
        cursor.execute('CREATE DATABASE data_visual_proj')

    return cursor

create_table_query = """
{
CREATE TABLE visualise_data(
id INT AUTO_INCREMENT PRIMARY KEY,
from INT,
to INT,
name VARCHAR(50),
type VARCHAR(50),
count INT,
amount FLOAT,
year INT,
state VARCHAR(50),
country VARCHAR(50)
);
}"""

try:
    cursor.execute(create_table_query)
except:
    pass
# def add_data_to_sql():
#     engine = create_engine('mysql+mysqlconnector://', creator=lambda: conn)
#     df.to_sql('data_visual_proj', con=engine, if_exists='replace', index=False)
#
