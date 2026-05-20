"""
scripts/03_load_bronze.py
─────────────────────────────────────────────────────────────────────────────
Charge les CSV Olist en Bronze via Parquet + MinIO.

Ordre d'exécution pour chaque table :
  1. Lire le CSV avec pandas
  2. Écrire en Parquet temporaire
  3. Uploader le Parquet dans MinIO (nessie-warehouse/bronze/<table>/)
  4. Créer la table Iceberg dans Trino (IF NOT EXISTS, pointe vers le dossier)
  5. Vérifier COUNT(*) via Trino

Prérequis :
  pip install -r requirements.txt   (boto3, trino, pandas, pyarrow)
  02_create_bronze_tables.py déjà exécuté (pour les schémas)

Usage :
  python scripts/03_load_bronze.py
  python scripts/03_load_bronze.py --table orders
"""

import pandas as pd
import boto3, os, argparse, time, tempfile
from trino.dbapi import connect

# ── Config ────────────────────────────────────────────────────────────────────
TRINO_HOST   = "localhost"
TRINO_PORT   = 8080
MINIO_URL    = "http://localhost:9000"
MINIO_KEY    = "admin"
MINIO_SECRET = "password123"
BUCKET       = "nessie-warehouse"
WAREHOUSE    = f"s3://{BUCKET}"
DATA_DIR     = "./data/olist"

# ── Définition des tables Bronze ──────────────────────────────────────────────
# (csv_file, table_name, colonnes, types_trino)
# types_trino : dict colonne → type SQL (None = VARCHAR par défaut)
LOADERS = [
    (
        "olist_orders_dataset.csv", "orders",
        ["order_id","customer_id","order_status","order_purchase_timestamp",
         "order_approved_at","order_delivered_carrier_date",
         "order_delivered_customer_date","order_estimated_delivery_date"],
        {},
    ),
    (
        "olist_order_items_dataset.csv", "order_items",
        ["order_id","order_item_id","product_id","seller_id",
         "shipping_limit_date","price","freight_value"],
        {"order_item_id":"INTEGER","price":"DOUBLE","freight_value":"DOUBLE"},
    ),
    (
        "olist_customers_dataset.csv", "customers",
        ["customer_id","customer_unique_id","customer_zip_code_prefix",
         "customer_city","customer_state"],
        {},
    ),
    (
        "olist_order_payments_dataset.csv", "order_payments",
        ["order_id","payment_sequential","payment_type",
         "payment_installments","payment_value"],
        {"payment_sequential":"INTEGER","payment_installments":"INTEGER",
         "payment_value":"DOUBLE"},
    ),
    (
        "olist_order_reviews_dataset.csv", "order_reviews",
        ["review_id","order_id","review_score","review_comment_title",
         "review_comment_message","review_creation_date","review_answer_timestamp"],
        {"review_score":"INTEGER"},
    ),
    (
        "olist_products_dataset.csv", "products",
        ["product_id","product_category_name","product_name_length",
         "product_description_length","product_photos_qty","product_weight_g",
         "product_length_cm","product_height_cm","product_width_cm"],
        {"product_name_length":"DOUBLE","product_description_length":"DOUBLE",
         "product_photos_qty":"DOUBLE","product_weight_g":"DOUBLE",
         "product_length_cm":"DOUBLE","product_height_cm":"DOUBLE",
         "product_width_cm":"DOUBLE"},
    ),
    (
        "olist_sellers_dataset.csv", "sellers",
        ["seller_id","seller_zip_code_prefix","seller_city","seller_state"],
        {},
    ),
    (
        "olist_geolocation_dataset.csv", "geolocation",
        ["geolocation_zip_code_prefix","geolocation_lat","geolocation_lng",
         "geolocation_city","geolocation_state"],
        {"geolocation_lat":"DOUBLE","geolocation_lng":"DOUBLE"},
    ),
    (
        "product_category_name_translation.csv", "category_translation",
        ["product_category_name","product_category_name_english"],
        {},
    ),
]

def get_s3():
    return boto3.client("s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1",
    )

def get_trino():
    return connect(
        host=TRINO_HOST, port=TRINO_PORT,
        user="admin", catalog="iceberg", schema="bronze",
    )

def run_sql(cur, sql):
    cur.execute(sql)
    try: cur.fetchall()
    except: pass

def load_table(s3, cur, csv_path, table, columns, type_overrides):
    if not os.path.exists(csv_path):
        print(f"  ⚠  Manquant : {csv_path} — ignoré")
        return 0

    # ── 1. Lire le CSV ────────────────────────────────────────────────────────
    df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    available = [c for c in columns if c in df.columns]
    df = df[available].copy()

    # Convertir les colonnes numériques
    for col, typ in type_overrides.items():
        if col in df.columns:
            if typ in ("DOUBLE", "INTEGER"):
                df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.where(pd.notnull(df), None)
    df["_loaded_at"] = pd.Timestamp.now()
    n_rows = len(df)

    # ── 2. Écrire Parquet ─────────────────────────────────────────────────────
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    df.to_parquet(tmp_path, index=False, engine="pyarrow")
    size_mb = os.path.getsize(tmp_path) / 1024 / 1024

    # ── 3. Uploader dans MinIO ────────────────────────────────────────────────
    s3_key = f"bronze/{table}/data.parquet"
    s3.upload_file(tmp_path, BUCKET, s3_key)
    os.unlink(tmp_path)
    print(f"    ✓ Parquet uploadé → s3://{BUCKET}/{s3_key} ({size_mb:.1f} Mo)")

    # ── 4. Créer la table Iceberg (si elle n'existe pas déjà) ─────────────────
    col_defs = []
    for c in available:
        typ = type_overrides.get(c, "VARCHAR")
        col_defs.append(f"    {c} {typ}")
    col_defs.append("    _loaded_at TIMESTAMP(6)")
    cols_sql = ",\n".join(col_defs)
    location = f"{WAREHOUSE}/bronze/{table}/"

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS iceberg.bronze.{table} (
        {cols_sql}
        ) WITH (
            format   = 'PARQUET',
            location = '{location}'
        )
    """
    try:
        run_sql(cur, create_sql)
        print(f"    ✓ Table iceberg.bronze.{table} créée/existante")
    except Exception as e:
        print(f"    ⚠  CREATE TABLE : {e}")

    # ── 5. Vérifier le COUNT ──────────────────────────────────────────────────
    try:
        cur.execute(f"SELECT COUNT(*) FROM iceberg.bronze.{table}")
        count = cur.fetchone()[0]
        status = "✓" if count > 0 else "⚠ "
        print(f"    {status} Trino voit {count:,} lignes dans bronze.{table}")
    except Exception as e:
        print(f"    ⚠  COUNT(*) : {e}")

    return n_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", help="Charger une seule table (ex: orders)")
    args = parser.parse_args()

    try:
        import pyarrow
    except ImportError:
        print("✗ pyarrow manquant → pip install pyarrow")
        raise SystemExit(1)

    s3   = get_s3()
    conn = get_trino()
    cur  = conn.cursor()
    print(f"✓ Connecté à Trino et MinIO\n")

    loaders = [(f,t,c,ty) for f,t,c,ty in LOADERS
               if not args.table or t == args.table]

    total, start = 0, time.time()
    for csv_file, table, columns, types in loaders:
        print(f"── {table:<30} ← {csv_file}")
        t0 = time.time()
        n  = load_table(s3, cur, f"{DATA_DIR}/{csv_file}", table, columns, types)
        print(f"    ⏱  {time.time()-t0:.1f}s\n")
        total += n

    print(f"✓ {total:,} lignes chargées en {time.time()-start:.0f}s")
    print("  Prochaine étape : ./run_pipeline.sh")

if __name__ == "__main__":
    main()