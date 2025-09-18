# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    items_table.py                                     :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: dsindres <dsindres@student.42.fr>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2025/09/16 13:58:30 by dsindres          #+#    #+#              #
#    Updated: 2025/09/16 14:49:42 by dsindres         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import pandas as pd
import psycopg2
from psycopg2 import sql

# 1. Lire le CSV
csv_file = "/goinfre/dsindres/subject/item/item.csv"
df = pd.read_csv(csv_file)

# 2. Connexion à PostgreSQL
conn = psycopg2.connect(
    dbname="piscineds",
    user="dsindres",
    password="mysecretpassword",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# 3. Créer la table si elle n'existe pas
#    Ici, on définit les types explicitement : id -> integer, name -> text, price -> real
cur.execute("""
CREATE TABLE IF NOT EXISTS items (
    product_id INTEGER PRIMARY KEY,
    category_id NUMERIC,
    category_code TEXT,
    brand TEXT
)
""")
conn.commit()

# 4. Insérer les données du CSV
for idx, row in df.iterrows():
    cur.execute("""
    INSERT INTO items (product_id, category_id, category_code, brand)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (product_id) DO NOTHING
    """, (row['product_id'], row['category_id'], row['category_code'], row['brand']))
conn.commit()

# 5. Fermer la connexion
cur.close()
conn.close()

print("Import terminé !")
