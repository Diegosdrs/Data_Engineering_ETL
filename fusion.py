# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    fusion.py                                          :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: dsindres <dsindres@student.42.fr>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2025/09/17 11:49:43 by dsindres          #+#    #+#              #
#    Updated: 2025/09/17 11:49:52 by dsindres         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import psycopg2
from psycopg2 import sql

def join_customers_items():
    # Connexion à la base PostgreSQL
    conn = psycopg2.connect(
        dbname="piscineds",
        user="dsindres",
        password="mysecretpassword",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    # Créer une nouvelle table pour stocker le résultat de la jointure
    cur.execute("""
    DROP TABLE IF EXISTS customers_full;
    CREATE TABLE customers_full AS
    SELECT c.*, i.category_id, i.category_code, i.brand
    FROM customers c
    LEFT JOIN items i
    ON c.product_id = i.product_id;
    """)
    conn.commit()

    print("Jointure terminée : table 'customers_full' créée avec succès !")

    cur.close()
    conn.close()


if __name__ == "__main__":
    join_customers_items()
