# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    clean_export_data.py                               :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: dsindres <dsindres@student.42.fr>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2025/09/15 14:32:39 by dsindres          #+#    #+#              #
#    Updated: 2025/09/18 10:48:58 by dsindres         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import numpy as np
import pandas as pd
from pathlib import Path
import os
import sys
import uuid
import re
from datetime import datetime
import psycopg2
from psycopg2 import sql
from psycopg2 import extras


# Verifier les fichiers + cleaner les datas
def clean_data(folder_clean_data):
    
    if not folder_clean_data.exists():
        folder = Path("/goinfre/dsindres/subject/customer/")
        df_file = []
        for file in folder.iterdir():
            if not file.is_file() or file.suffix != ".csv" or not file.exists() or file.stat().st_size == 0:
                print("Error : fichier corrompu")
                sys.exit(1)
            df_file.append(str(file))
            
        for file in df_file:
            df = pd.read_csv(file)
            if verif_data(df, file) == 1:
                print(f"Error : fichier ${file} pas au bon format")
                sys.exit(1)

# Verification des datas (bon format, bon type etc...)
# convertir en np.ndarray pour eviter d'utiliser enumerate qui freeze sur un grand nombre de donnee
def verif_data(df: pd.DataFrame, file_name: str) -> int:
    expected_cols = ["event_time", "event_type", "product_id", "price", "user_id", "user_session"]
    if list(df.columns) != expected_cols:
        return 1

    uuid_pattern = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')

    valid_df = []
    df_array = df.values  # Convertir en numpy array = plus rapide
    columns = df.columns.tolist()
    
    # Utiliser enumerate sur les valeurs numpy au lieu d'iterrows
    for idx, row_values in enumerate(df_array):
        skip_line = False
        row_dict = dict(zip(columns, row_values))  # Recréer le format row
        
        for col_idx, col in enumerate(columns):
            if col == 'user_session':
                if not uuid_pattern.match(str(row_dict[col])):
                    row_dict[col] = str(uuid.uuid4())
            elif col == 'price':
                if pd.isna(row_dict[col]):
                    skip_line = True
                    break
                else:
                    try:
                        row_dict[col] = float(row_dict[col])
                    except ValueError:
                        skip_line = True
                        break
            elif col == "user_id":
                if len(str(row_dict[col])) != 9:
                    skip_line = True
                    break
                try:
                    row_dict[col] = int(row_dict[col])
                except ValueError:
                    skip_line = True
                    break
            elif col == "product_id":
                try:
                    row_dict[col] = int(row_dict[col])
                except ValueError:
                    skip_line = True
                    break
            elif col == "event_type":
                if row_dict[col] != "cart" and row_dict[col] != "view" and row_dict[col] != "remove_from_cart":
                    skip_line = True
                    break
            elif col == "event_time":
                try:
                    row_dict[col] = datetime.strptime(str(row_dict[col]).replace(" UTC", ""), "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    skip_line = True
                    break
        
        if not skip_line:
            valid_df.append(row_dict)

    # Créer le DataFrame une seule fois à la fin
    # la liste de dictionnaire devient un dataframe (cle devient colonne, chaque dico une liste)
    valid_df = pd.DataFrame(valid_df)

    first_part, last_part = os.path.split(file_name)
    output_dir = os.path.join(first_part, "customer_good_format")
    os.makedirs(output_dir, exist_ok=True)
    prefixe = "clean_"
    complete_data_name = os.path.join(output_dir, prefixe + last_part.replace(".csv", ".parquet"))
    valid_df.to_parquet(complete_data_name, index=False, compression="snappy")
    return 0


# Mettre les fichiers csv par ordre chrono (peut etre optimise)
def take_in_order(parquet_files: list) -> list:
    # Ordre des années
    years_order = ["2021", "2022", "2023", "2024"]

    # Ordre des mois (abréviations dans tes noms de fichiers parquet)
    months_order = ["oct", "nov", "dec", "jan", "fev", "mars"]

    # On définit une fonction clé pour trier correctement
    def sort_key(file_path):
        name = file_path.stem  # ex: "clean_data_2022_dec"
        parts = name.split("_")  # ["clean", "data", "2022", "dec"]
        year = parts[2]
        month = parts[3]

        return (years_order.index(year), months_order.index(month))

    # Trier la liste
    df_in_order = sorted(parquet_files, key=sort_key)

    return df_in_order


def concatenate_data(folder_clean_data) -> pd.DataFrame:
    
    # concatener tous les fichiers en un seul
    parquet_files = list(folder_clean_data.glob("*.parquet"))
    if not parquet_files:
        print("Error : fichier nettoye introuvable")
        sys.exit(1)
    
    sorted_files = take_in_order(parquet_files)
    all_dfs = [pd.read_parquet(pf) for pf in sorted_files]

    customers_df = pd.concat(all_dfs, ignore_index=True)
    return customers_df
    
    
def export_to_SQL(customers_df):

    #connexion a la base de donnees PostgreSQL
    conn = psycopg2.connect(
        dbname="piscineds",
        user="dsindres",
        password="mysecretpassword",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    
    #creation de la table dans PostgreSQL pour y stocker les datas
    cur.execute("DROP TABLE IF EXISTS customers")
    cur.execute("""
    CREATE TABLE customers (
        id SERIAL PRIMARY KEY,      -- Clé primaire auto-incrémentée pour unicité
        event_time TIMESTAMP,       -- Plus de contrainte unique sur ce champ
        event_type TEXT,
        product_id INTEGER,
        price FLOAT,
        user_id NUMERIC,
        user_session UUID
    )
    """)
    conn.commit()

    # Conversion explicite vers les types Python natifs pour éviter les problèmes numpy
    customers_df["event_time"] = customers_df["event_time"].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Maintenant on peut créer les records en toute sécurité
    records = customers_df.to_records(index=False)

  
  
    # BOUCLE POUR INSERER customer_df dans POSTGRESQL
    # Insertion par CHUNK car trop gros volume
    chunk_size = 50000
    total_records = len(records)
    inserted_count = 0

    insert_query = """
    INSERT INTO customers (event_time, event_type, product_id, price, user_id, user_session)
    VALUES %s
    """

    for i in range(0, total_records, chunk_size):
        chunk_records = records[i:i + chunk_size]
        values = []
        for record in chunk_records:
            converted_tuple = (
                str(record[0]),      # event_time (déjà converti en string)
                str(record[1]),      # event_type 
                int(record[2]),      # product_id - conversion forcée vers int Python
                float(record[3]),    # price - conversion forcée vers float Python
                int(record[4]),      # user_id - conversion forcée vers int Python
                str(record[5])       # user_session
            )
            values.append(converted_tuple)
        try:
            psycopg2.extras.execute_values(cur, insert_query, values, page_size=1000)
            conn.commit()
            inserted_count += len(values)
            print(f"Inséré: {inserted_count}/{total_records} lignes ({inserted_count/total_records*100:.1f}%)")
        except Exception as e:
            print(f"Erreur lors de l'insertion du chunk {i//chunk_size + 1}: {e}")
            conn.rollback()
            break
    
    cur.close()
    conn.close()

    print("import termine !")



def main():
    folder_clean_data = Path("/goinfre/dsindres/subject/customer/customer_good_format/")
    clean_data(folder_clean_data)
    customers_df = concatenate_data(folder_clean_data)
    export_to_SQL(customers_df)
    
if __name__ == "__main__":
    main()