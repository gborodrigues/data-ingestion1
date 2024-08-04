import mysql.connector
import os
import csv

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


def read_csv_files_in_directory(directory_path, encoding='utf-8'):
    files = os.listdir(directory_path)
    
    csv_files = [file for file in files if file.endswith('.csv')]
    
    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        try:
            print(f"Reading file: {file_path}")
            qtd = 0

            with open(file_path, mode='r', newline='', encoding=encoding) as file:
                csv_reader = csv.reader(file)
                header = next(csv_reader, None)
                
                if header is None:
                    print(f"Skipping empty file: {file_path}")
                    continue

                has_rows = False
                
                for row_number, row in enumerate(csv_reader, start=2):
                    has_rows = True
                    try:
                        # Processing each row
                        qtd += 1
                    except Exception as row_err:
                        print(f"Error processing row {row_number} in file {file_path}: {row_err}")
                        print(f"Row content: {row}")
                
                if not has_rows:
                    print(f"Skipping empty file (only header present): {file_path}")
                    continue

            print(f"File: {file_path} has {qtd} lines")
        except UnicodeDecodeError as e:
            print(f"Skipping file {file_path} due to encoding error: {e}")
        except Exception as file_err:
            print(f"Error reading file {file_path}: {file_err}")


if __name__ == "__main__":
    try:
        connect_to_db()
        directories_paths = ['Empregados', 'Bancos', 'ReclamaçΣes']
        for diretory in directories_paths:
            read_csv_files_in_directory(diretory)
        print('fim')
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")