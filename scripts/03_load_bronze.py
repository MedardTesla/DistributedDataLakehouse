"""
scripts/03_load_bronze.py
Insère les CSV Olist dans les tables Iceberg Bronze via Trino.
Batch de 500 lignes pour éviter les timeouts.

Usage :
  python scripts/03_load_bronze.py
  python scripts/03_load_bronze.py --table orders
"""

import pandas as pd
from trino.dbapi import connect
from datetime import datetime
import sys, argparse, time, os

TRINO_HOST = "localhost"
TRINO_PORT = 8080
DATA_DIR   = "./data/olist"
BATCH_SIZE = 500

LOADERS = [
    ("olist_orders_dataset.csv", "orders",
     ["order_id","customer_id","order_status","order_purchase_timestamp",
      "order_approved_at","order_delivered_carrier_date",
      "order_delivered_customer_date","order_estimated_delivery_date"]),

    ("olist_order_items_dataset.csv", "order_items",
     ["order_id","order_item_id","product_id","seller_id",
      "shipping_limit_date","price","freight_value"]),

    ("olist_customers_dataset.csv", "customers",
     ["customer_id","customer_unique_id","customer_zip_code_prefix",
      "customer_city","customer_state"]),

    ("olist_order_payments_dataset.csv", "order_payments",
     ["order_id","payment_sequential","payment_type",
      "payment_installments","payment_value"]),

    ("olist_order_reviews_dataset.csv", "order_reviews",
     ["review_id","order_id","review_score","review_comment_title",
      "review_comment_message","review_creation_date","review_answer_timestamp"]),

    ("olist_products_dataset.csv", "products",
     ["product_id","product_category_name","product_name_length",
      "product_description_length","product_photos_qty",
      "product_weight_g","product_length_cm","product_height_cm","product_width_cm"]),

    ("olist_sellers_dataset.csv", "sellers",
     ["seller_id","seller_zip_code_prefix","seller_city","seller_state"]),

    ("olist_geolocation_dataset.csv", "geolocation",
     ["geolocation_zip_code_prefix","geolocation_lat",
      "geolocation_lng","geolocation_city","geolocation_state"]),

    ("product_category_name_translation.csv", "category_translation",
     ["product_category_name","product_category_name_english"]),
]

def load_table(cur, csv_path, table, columns, loaded_at):
    if not os.path.exists(csv_path):
        print(f"  ⚠  Manquant : {csv_path} — ignoré")
        return 0

    df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    available = [c for c in columns if c in df.columns]
    df = df[available].fillna("")

    all_cols = available + ["_loaded_at"]
    # _loaded_at doit etre caste en TIMESTAMP(6) — Trino ne convertit pas VARCHAR auto
    data_ph = ", ".join(["?"] * len(available))
    sql = f"INSERT INTO bronze.{table} ({', '.join(all_cols)}) VALUES ({data_ph}, CAST(? AS TIMESTAMP(6)))"

    total, batch = 0, []
    for _, row in df.iterrows():
        vals = [str(row[c]) if row[c] != "" else None for c in available]
        vals.append(loaded_at)
        batch.append(vals)
        if len(batch) >= BATCH_SIZE:
            for v in batch: cur.execute(sql, v)
            total += len(batch); batch = []
            print(f"    {total:>7} / {len(df)}", end="\r")

    for v in batch: cur.execute(sql, v)
    total += len(batch)
    return total

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", help="Charger une seule table (ex: orders)")
    args = parser.parse_args()

    loaded_at = datetime.now().isoformat(sep=" ", timespec="seconds")
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="admin",
                   catalog="iceberg", schema="bronze")
    cur  = conn.cursor()
    print(f"✓ Connecté à Trino — {loaded_at}\n")

    loaders = [(f,t,c) for f,t,c in LOADERS if not args.table or t == args.table]
    total_rows, start = 0, time.time()

    for csv_file, table, columns in loaders:
        print(f"── {table:<30} ← {csv_file}")
        t0 = time.time()
        n  = load_table(cur, f"{DATA_DIR}/{csv_file}", table, columns, loaded_at)
        print(f"    ✓ {n:>7} lignes  ({time.time()-t0:.1f}s)          ")
        total_rows += n

    print(f"\n✓ {total_rows:,} lignes chargées en {time.time()-start:.0f}s")
    print("  Prochaine étape : ./run_pipeline.sh")

if __name__ == "__main__":
    main()