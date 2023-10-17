from flask import Flask, render_template, request
import pandas as pd
import chardet
import hashlib
import pyodbc
import os
import time
app = Flask(__name__)

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

def calculate_hash(row_values):
    hash_object = hashlib.sha1(','.join(str(val) for val in row_values).encode())
    return hash_object.hexdigest()

def check_and_update_table(cursor, db_table_name, columns_to_keep):
    connection_string = "DRIVER={SQL Server Native Client 11.0};SERVER=LAPTOP-IMUV9T4L;DATABASE=TestDB;Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes"
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{db_table_name}'")
    if cursor.fetchone():
        print("Table exists")
       
    else:
        print("Table doesn't exist")
        create_table_sql = f"""CREATE TABLE {db_table_name} (
            [NEW_PRIMARY_KEY] INT IDENTITY(1, 1) PRIMARY KEY,
            [RECORD_HASH] NVARCHAR(40),
            {', '.join([f'[{col}] NVARCHAR(MAX)' for col in columns_to_keep])}
        )"""
        print("create_table_sql:", create_table_sql)
        cursor.execute(create_table_sql)
        connection.commit()
        print("table created")
        

 


def check_and_insert_rows(cursor, connection, df, db_table_name, columns_to_keep):
    for index, row in df.iterrows():
        row_values = [str(val).replace("'", "''") if not pd.isna(val) else None for col, val in row.items() if col != 'NEW_PRIMARY_KEY']

        # Calculate the hash of the row values
        row_hash = calculate_hash(row_values)

        # Check if the same hash already exists in the database
        existing_query = f"SELECT COUNT(*) FROM {db_table_name} WHERE RECORD_HASH = ?"
        cursor.execute(existing_query, (row_hash,))
        if cursor.fetchone()[0] == 0:
            try:
                # Insert the row with the calculated hash 
                insert_statement = f"INSERT INTO {db_table_name} ({', '.join(columns_to_keep)}, RECORD_HASH) VALUES ({', '.join(['?' for _ in row_values])}, ?)"
                cursor.execute(insert_statement, tuple(row_values + [row_hash]))
                connection.commit()
                print("Insertion completed")
            except pyodbc.Error as ex:
                print("Error executing INSERT statement:", ex)
                connection.rollback()
        




def keep_selected_columns(input_file, columns_to_keep, encoding,db_table_name, date_filter=None, delimiter=';'):
    df = pd.read_csv(input_file, encoding=encoding, sep=delimiter) if input_file.endswith('.csv') else pd.read_excel(input_file)

    df.columns = df.columns.str.upper()
    columns_to_keep = [col.upper() for col in columns_to_keep]

    if not all(col in df.columns for col in columns_to_keep):
        missing_cols = [col for col in columns_to_keep if col not in df.columns]
        raise KeyError(f"Columns not found in DataFrame: {missing_cols}")

    df = df[columns_to_keep]
    print(df)

    if date_filter is not None:
        date_column = date_filter[2]
        df[date_column] = pd.to_datetime(df[date_column], format='%d/%m/%Y', errors='coerce')
        df = df[(df[date_column] >= pd.to_datetime(date_filter[0], format='%d/%m/%Y')) & (df[date_column] <= pd.to_datetime(date_filter[1], format='%d/%m/%Y'))]

    connection_string = "DRIVER={SQL Server Native Client 11.0};SERVER=LAPTOP-IMUV9T4L;DATABASE=TestDB;Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes"
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()


    check_and_update_table(cursor, db_table_name, columns_to_keep)
    time.sleep(2)
    



    table_schema = {
        col: 'NVARCHAR(255)' for col in columns_to_keep
    }



    df['NEW_PRIMARY_KEY'] = range(1, len(df) + 1)


    insert_columns = [col for col in columns_to_keep if col != 'NEW_PRIMARY_KEY']
    placeholders = ', '.join(['?' for _ in insert_columns])
    insert_statement = f"INSERT INTO {db_table_name} ({', '.join(insert_columns)}) VALUES ({placeholders})"

    check_and_insert_rows(cursor, connection, df, db_table_name, columns_to_keep)

    connection.close()

    return



def search_file_with_chain(directory, character_chain):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if character_chain in file:
                return os.path.join(root, file)
    return None

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/process_file', methods=['POST'])
def process_file():
    directory = request.form['directory']
    character_chain = request.form['character_chain']
    input_file = search_file_with_chain(directory, character_chain)

    if input_file:
        columns_input = request.form['columns_input']
        columns_to_keep = [col.strip() for col in columns_input.split(',')]
        date_input = request.form['date_input']
        date_filter = [date.strip() for date in date_input.split(',')] if date_input else None
        delimiter = request.form['delimiter']
        table_name = request.form['table_name']

        encoding = detect_encoding(input_file)

        tsql_insert = keep_selected_columns(input_file, columns_to_keep, encoding, table_name, date_filter, delimiter)
        return render_template('index.html', tsql_insert=tsql_insert)
    else:
        return "File with the given character chain not found in the directory."

if __name__ == '__main__':
    app.run(debug=True)