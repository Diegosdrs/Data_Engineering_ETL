# Data Engineering ETL

## 📌 Objectif du projet
L’objectif était de mettre en place un pipeline complet permettant de :
- Nettoyer des fichiers CSV bruts (données clients & événements).
- Vérifier la validité et la cohérence des données (formats, types, UUID, etc.).
- Convertir les fichiers nettoyés en **Parquet** (format optimisé).
- Concaténer l’ensemble des fichiers dans un seul dataset structuré.
- Charger ces données dans une base **PostgreSQL**.
- Visualiser et manipuler facilement les données avec **DBeaver**.

---

## ⚙️ Technologies utilisées
- **Python 3** : nettoyage, transformation et insertion en base.  
  - Pandas, Numpy → manipulation des datasets.  
  - Psycopg2 → connexion et insertion dans PostgreSQL.  
- **PostgreSQL** : stockage des données propres et consolidées.  
- **DBeaver** : outil graphique pour explorer les tables et exécuter des requêtes SQL.  

---

## 📚 Ce que j’ai appris
- Création et gestion de tables PostgreSQL (clés primaires, types de colonnes).  
- Manipulation avancée de DataFrames avec **Pandas**.  
- Conversion CSV → Parquet pour optimiser la performance.  
- Détection et suppression des doublons dans une base SQL.  
- Gestion de l’insertion massive de données avec `execute_values` (batch insert).  
- Utilisation de **DBeaver** pour visualiser efficacement les données stockées.  

---

## 🚀 Workflow du projet
1. **Nettoyage** : vérification des colonnes, conversion des types, correction des UUID.  
2. **Transformation** : sauvegarde en fichiers Parquet, tri par année/mois.  
3. **Concaténation** : fusion de tous les fichiers dans un seul DataFrame `customers`.  
4. **Insertion SQL** : création de la table PostgreSQL et insertion des enregistrements.  
5. **Visualisation** : exploration de la base avec DBeaver.  

---

## 📂 Structure principale
- `clean_export_data.py` → script principal de nettoyage et export vers PostgreSQL.  
- `items_table.py` → création et import de la table `items`.  
- `automatisation_csv.py` → automatisation du pipeline complet.  

---

## ✅ Résultat final
Une base PostgreSQL consolidée contenant l’ensemble des événements clients, nettoyée, sans doublons, et prête à être utilisée pour des analyses plus avancées.
