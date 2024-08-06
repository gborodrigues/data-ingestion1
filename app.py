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
    dataframes = []
    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        try:
            if directory_path == "Bancos":
                df = pd.read_csv(file_path, sep='\t', encoding='latin-1')
            elif directory_path == "Empregados":
                df = pd.read_csv(file_path, sep='|', encoding='latin-1')
            else:
                df = pd.read_csv(file_path, sep=';', encoding='latin-1')
            dataframes.append(df)
        except pd.errors.EmptyDataError:
            print(f"Skipping empty file: {file_path}")
        except Exception as file_err:
            raise (f"Error reading file {file_path}: {file_err}")
    merged_df = pd.concat(dataframes, ignore_index=True)
    return merged_df


def clean_string(df, field):
    pattern = (
        r' - PRUDENCIAL|'
        r' S\.A[./]?|'
        r' S/A[/]?|'
        r'GRUPO|'
        r' SCFI|'
        r' CC |'
        r' C\.C |'
        r' CCTVM[/]?|'
        r' LTDA[/]?|'
        r' DTVM[/]?|'
        r' BM[/]?|'
        r' CH[/]?|'
        r'COOPERATIVA DE CRÉDITO, POUPANÇA E INVESTIMENTO D[E?O?A/]?|'
        r' [(]conglomerado[)]?|'
        r'GRUPO[ /]|'
        r' -[ /]?'
    )

    df["campo_limpo"] = df[field].str.replace(pattern, '', regex=True)
    return df


if __name__ == "__main__":
    try:
        connect_to_db()
        directories_paths = ['Bancos', 'Empregados', 'ReclamaçΣes']
        dataframes = {}
        for diretory in directories_paths:
            dataframe = read_csv_files_in_directory(diretory)
            dataframes[diretory] = dataframe
        dataframes['Bancos'] = clean_string(dataframes['Bancos'], 'Nome')
        dataframes['Empregados'] = clean_string(dataframes['ReclamaçΣes'], 'Instituição financeira')
        merged_df = pd.merge(dataframes['Bancos'], dataframes['ReclamaçΣes'], on=["campo_limpo"])
        print(merged_df)
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")