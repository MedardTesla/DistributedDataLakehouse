"""
scripts/03_load_bronze.py
─────────────────────────────────────────────────────────────────────────────
Charge les CSV Olist dans les tables Iceberg Bronze via INSERT INTO Trino.

Stratégie robuste :
  - Batch de 100 lignes par INSERT VALUES (évite les timeouts)
  - Reconnexion automatique toutes les 5000 lignes
  - Trino écrit lui-même les fichiers Parquet + métadonnées Iceberg
  → Pas de problème de fichiers "orphelins" non reconnus par Iceberg

Prérequis :
  pip install -r requirements.txt
  02_create_bronze_tables.py déjà exécuté (schémas + tables vides)

Usage :
  python scripts/03_load_bronze.py
  python scripts/03_load_bronze.py --table orders
"""

import pandas as pd
import argparse, time, sys
from trino.dbapi import connect

TRINO_HOST  = "localhost"
TRINO_PORT  = 8080
DATA_DIR    = "./data/olist"

# Batch size : 100 lignes par INSERT VALUES → évite les timeouts
# Reconnexion toutes les RECONNECT_EVERY lignes
BATCH_SIZE     = 100
RECONNECT_EVERY = 5000

LOADERS = [
    ("olist_orders_dataset.csv", "orders",
     ["order_id","customer_id","order_status","order_purchase_timestamp",
      "order_approved_at","order_delivered_carrier_date",
      "order_delivered_customer_date","order_estimated_delivery_date"],
     {}),

    ("olist_order_items_dataset.csv", "order_items",
     ["order_id","order_item_id","product_id","seller_id",
      "shipping_limit_date","price","freight_value"],
     {"order_item_id":"INTEGER","price":"DOUBLE","freight_value":"DOUBLE"}),

    ("olist_customers_dataset.csv", "customers",
     ["customer_id","customer_unique_id","customer_zip_code_prefix",
      "customer_city","customer_state"], {}),

    ("olist_order_payments_dataset.csv", "order_payments",
     ["order_id","payment_sequential","payment_type",
      "payment_installments","payment_value"],
     {"payment_sequential":"INTEGER","payment_installments":"INTEGER",
      "payment_value":"DOUBLE"}),

    ("olist_order_reviews_dataset.csv", "order_reviews",
     ["review_id","order_id","review_score","review_comment_title",
      "review_comment_message","review_creation_date","review_answer_timestamp"],
     {"review_score":"INTEGER"}),

    ("olist_products_dataset.csv", "products",
     ["product_id","product_category_name","product_name_length",
      "product_description_length","product_photos_qty","product_weight_g",
      "product_length_cm","product_height_cm","product_width_cm"],
     {"product_name_length":"DOUBLE","product_description_length":"DOUBLE",
      "product_photos_qty":"DOUBLE","product_weight_g":"DOUBLE",
      "product_length_cm":"DOUBLE","product_height_cm":"DOUBLE",
      "product_width_cm":"DOUBLE"}),

    ("olist_sellers_dataset.csv", "sellers",
     ["seller_id","seller_zip_code_prefix","seller_city","seller_state"], {}),

    ("olist_geolocation_dataset.csv", "geolocation",
     ["geolocation_zip_code_prefix","geolocation_lat","geolocation_lng",
      "geolocation_city","geolocation_state"],
     {"geolocation_lat":"DOUBLE","geolocation_lng":"DOUBLE"}),

    ("product_category_name_translation.csv", "category_translation",
     ["product_category_name","product_category_name_english"], {}),
]

def new_conn():
    return connect(host=TRINO_HOST, port=TRINO_PORT,
                   user="admin", catalog="iceberg", schema="bronze",
                   request_timeout=120)

def fmt_val(v, sql_type):
    """Formate une valeur Python en littéral SQL Trino."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "NULL"
    if sql_type in ("DOUBLE",):
        try: return str(float(v))
        except: return "NULL"
    if sql_type == "INTEGER":
        try: return str(int(float(v)))
        except: return "NULL"
    if sql_type == "TIMESTAMP(6)":
        return f"TIMESTAMP '{v}'"
    # VARCHAR — échapper les apostrophes
    s = str(v).replace("'", "''")
    return f"'{s}'"

def load_table(table, columns, type_overrides, csv_path):
    import os
    if not os.path.exists(csv_path):
        print(f"  ⚠  Manquant : {csv_path}"); return 0

    df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    available = [c for c in columns if c in df.columns]
    df = df[available].fillna("")

    loaded_at = pd.Timestamp.now().isoformat(sep=" ", timespec="microseconds")

    # Colonnes + types dans l'ordre INSERT
    all_cols  = available + ["_loaded_at"]
    all_types = [type_overrides.get(c, "VARCHAR") for c in available] + ["TIMESTAMP(6)"]
    cols_str  = ", ".join(all_cols)

    # Supprimer les données existantes avant de charger
    conn = new_conn()
    cur  = conn.cursor()
    try:
        cur.execute(f"DELETE FROM bronze.{table}")
        cur.fetchall()
        print(f"    ✓ Table bronze.{table} vidée")
    except Exception as e:
        print(f"    ⚠  DELETE (ignoré si table vide) : {e}")

    total = 0
    rows  = df.values.tolist()
    n     = len(rows)

    i = 0
    while i < n:
        # Reconnexion périodique pour éviter les timeouts
        if i > 0 and i % RECONNECT_EVERY == 0:
            try: conn.close()
            except: pass
            conn = new_conn()
            cur  = conn.cursor()

        batch = rows[i : i + BATCH_SIZE]

        # Construire INSERT VALUES multi-lignes
        value_rows = []
        for row in batch:
            vals = []
            for j, col in enumerate(available):
                vals.append(fmt_val(row[j], all_types[j]))
            vals.append(fmt_val(loaded_at, "TIMESTAMP(6)"))
            value_rows.append(f"({', '.join(vals)})")

        sql = f"INSERT INTO bronze.{table} ({cols_str}) VALUES\n" + ",\n".join(value_rows)

        try:
            cur.execute(sql)
            cur.fetchall()
            total += len(batch)
        except Exception as e:
            print(f"\n    ✗ Erreur batch {i}-{i+len(batch)} : {e}")
            # Continuer avec le batch suivant
            i += BATCH_SIZE
            continue

        i += BATCH_SIZE
        pct = total * 100 // n
        bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
        print(f"    [{bar}] {total:>7}/{n} lignes", end="\r")

    print(f"    {'█'*20} {total:>7}/{n} lignes    ")

    # Vérification finale
    try:
        cur.execute(f"SELECT COUNT(*) FROM bronze.{table}")
        count = cur.fetchone()[0]
        icon = "✓" if count > 0 else "⚠ "
        print(f"    {icon} Trino confirme : {count:,} lignes dans bronze.{table}")
    except Exception as e:
        print(f"    ⚠  Vérification : {e}")

    try: conn.close()
    except: pass
    return total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", help="Charger une seule table (ex: orders)")
    args = parser.parse_args()

    loaders = [(f,t,c,ty) for f,t,c,ty in LOADERS
               if not args.table or t == args.table]

    print(f"✓ Chargement Bronze via INSERT Trino ({BATCH_SIZE} lignes/batch)\n")

    total_rows, start = 0, time.time()

    for csv_file, table, columns, types in loaders:
        import os
        csv_path = f"{DATA_DIR}/{csv_file}"
        print(f"── {table:<30} ({os.path.getsize(csv_path)//1024} Ko)")
        t0 = time.time()
        n  = load_table(table, columns, types, csv_path)
        print(f"    ⏱  {time.time()-t0:.0f}s\n")
        total_rows += n

    elapsed = time.time() - start
    m, s = divmod(int(elapsed), 60)
    print(f"✓ {total_rows:,} lignes insérées en {m}m{s:02d}s")
    print("  Prochaine étape : ./run_pipeline.sh")

if __name__ == "__main__":
    main()