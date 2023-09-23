from sqlalchemy import create_engine
import streamlit as st

create_table_query = """
CREATE TABLE visualise_data(
id INT AUTO_INCREMENT PRIMARY KEY,
name VARCHAR(50),
type VARCHAR(50),
count BIGINT,
amount DOUBLE,
year INT,
state VARCHAR(50),
country VARCHAR(50),
from1 BIGINT,
to1 BIGINT
)"""

def add_data_to_sql(df, conn, cursor):
    for index, row in df.iterrows():
        print(row)
        insert_query = """
            INSERT INTO visualise_data
            (name, type, count, amount, year, state, country, from1, to1)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
        cursor.execute(insert_query, (row['name'], row['type'], row['count'], row['amount'], row['year'], row['state'], row['country'],row['from1'], row['to1'] ))
    # engine = create_engine('mysql+mysqlconnector://', creator=lambda: conn)
    # df.to_sql('visualise_data', con=engine, if_exists='replace', index=False)

def query_sql(cursor, conn):
    cursor.execute('SELECT * FROM visualise_data')
    all_data = cursor.fetchall()
    for row in all_data:
        # print(row)
        pass
    # st.write('column name :', all_data.columns.tolist())

    st.title('SQL data display')
    st.table(all_data)

