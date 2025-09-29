# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    supp_doublons.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: dsindres <dsindres@student.42.fr>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2025/09/17 11:10:33 by dsindres          #+#    #+#              #
#    Updated: 2025/09/23 11:03:53 by dsindres         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import psycopg2

def remove_duplicates():
    conn = psycopg2.connect(
        dbname="piscineds",
        user="dsindres",
        password="mysecretpassword",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    # Supprimer les doublons stricts
    cur.execute("""
    DELETE FROM customers a
    USING (
        SELECT ctid, ROW_NUMBER() OVER (
            PARTITION BY event_time, event_type, product_id, price, user_id, user_session
            ORDER BY event_time
        ) AS rn
        FROM customers
    ) b
    WHERE a.ctid = b.ctid
    AND b.rn > 1;
    """)

    # Supprimer les doublons à 1 seconde près
    cur.execute("""
    DELETE FROM customers a
    USING (
        SELECT ctid, ROW_NUMBER() OVER (
            PARTITION BY event_type, product_id, user_id, user_session, date_trunc('second', event_time)::timestamp
            ORDER BY event_time
        ) AS rn
        FROM customers
    ) b
    WHERE a.ctid = b.ctid
    AND b.rn > 1;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Nettoyage terminé !")



def main():
    remove_duplicates()
  

if __name__ == "__main__":
    main()