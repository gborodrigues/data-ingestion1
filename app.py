import mysql.connector
import os
import pandas as pd

def connect_to_db():
    db_config = {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'database': os.getenv('DB_DATABASE')
    }

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT DATABASE();")
        record = cursor.fetchone()
        print("You're connected: ", record)
    except Exception as err:
        print(f"Error: {err}")


def read_csv_files_in_directory(directory_path):
    files = os.listdir(directory_path)
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        try:
            print(f"Reading file: {file_path}")
            df = pd.read_csv(file_path, sep='|', encoding='latin-1')
            qtd = len(df)
            print(f"File: {file_path} has {qtd} lines")
        except UnicodeDecodeError:
            if df.empty:
                print(f"Skipping empty file: {file_path}")
                continue
        except Exception as file_err:
            print(f"Error reading file {file_path}: {file_err}")


if __name__ == "__main__":
    try:
        connect_to_db()
        directories_paths = ['Bancos', 'Empregados', 'ReclamaçΣes']
        for diretory in directories_paths:
            read_csv_files_in_directory(diretory)
        print('fim')
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")