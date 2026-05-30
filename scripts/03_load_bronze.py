"""
scripts/03_load_bronze.py
─────────────────────────────────────────────────────────────────────────────
Charge les CSV Olist en Bronze via PyArrow + MinIO + Nessie REST API.

Stratégie ultra-rapide (pas d'INSERT Trino) :
  1. Lire le CSV avec pandas
  2. Écrire un fichier Parquet optimisé avec PyArrow
  3. Uploader dans MinIO
  4. Enregistrer la table Iceberg directement via l'API REST de Nessie
     (PyIceberg crée les métadonnées sans passer par Trino)
  5. Vérifier via Trino SELECT COUNT(*)

Temps estimé : 30 secondes pour les 9 tables (~1.5 millions de lignes)

Prérequis :
  pip install -r requirements.txt

Usage :
  python scripts/03_load_bronze.py
  python scripts/03_load_bronze.py --table orders
  python scripts/03_load_bronze.py --reset   # supprime et recharge tout
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import requests
import json
import argparse
import time
import os
import sys
import tempfile
from trino.dbapi import connect

# ── Config ─────────────────────────────────────────────────────────────────
TRINO_HOST   = "localhost"
TRINO_PORT   = 8080
MINIO_URL    = "http://localhost:9000"
MINIO_KEY    = "admin"
MINIO_SECRET = "password123"
BUCKET       = "nessie-warehouse"
NESSIE_URL   = "http://localhost:19120/api/v1"
DATA_DIR     = "./data/olist"

# ── Schémas PyArrow par table ───────────────────────────────────────────────
# Définit les types exacts — PyArrow écrit un Parquet typé que Trino lira sans erreur

SCHEMAS = {
    "orders": pa.schema([
        ("order_id",                       pa.string()),
        ("customer_id",                    pa.string()),
        ("order_status",                   pa.string()),
        ("order_purchase_timestamp",       pa.string()),
        ("order_approved_at",              pa.string()),
        ("order_delivered_carrier_date",   pa.string()),
        ("order_delivered_customer_date",  pa.string()),
        ("order_estimated_delivery_date",  pa.string()),
        ("_loaded_at",                     pa.timestamp("us")),
    ]),
    "order_items": pa.schema([
        ("order_id",           pa.string()),
        ("order_item_id",      pa.int32()),
        ("product_id",         pa.string()),
        ("seller_id",          pa.string()),
        ("shipping_limit_date",pa.string()),
        ("price",              pa.float64()),
        ("freight_value",      pa.float64()),
        ("_loaded_at",         pa.timestamp("us")),
    ]),
    "customers": pa.schema([
        ("customer_id",              pa.string()),
        ("customer_unique_id",       pa.string()),
        ("customer_zip_code_prefix", pa.string()),
        ("customer_city",            pa.string()),
        ("customer_state",           pa.string()),
        ("_loaded_at",               pa.timestamp("us")),
    ]),
    "order_payments": pa.schema([
        ("order_id",             pa.string()),
        ("payment_sequential",   pa.int32()),
        ("payment_type",         pa.string()),
        ("payment_installments", pa.int32()),
        ("payment_value",        pa.float64()),
        ("_loaded_at",           pa.timestamp("us")),
    ]),
    "order_reviews": pa.schema([
        ("review_id",               pa.string()),
        ("order_id",                pa.string()),
        ("review_score",            pa.int32()),
        ("review_comment_title",    pa.string()),
        ("review_comment_message",  pa.string()),
        ("review_creation_date",    pa.string()),
        ("review_answer_timestamp", pa.string()),
        ("_loaded_at",              pa.timestamp("us")),
    ]),
    "products": pa.schema([
        ("product_id",                  pa.string()),
        ("product_category_name",       pa.string()),
        ("product_name_length",         pa.float64()),
        ("product_description_length",  pa.float64()),
        ("product_photos_qty",          pa.float64()),
        ("product_weight_g",            pa.float64()),
        ("product_length_cm",           pa.float64()),
        ("product_height_cm",           pa.float64()),
        ("product_width_cm",            pa.float64()),
        ("_loaded_at",                  pa.timestamp("us")),
    ]),
    "sellers": pa.schema([
        ("seller_id",              pa.string()),
        ("seller_zip_code_prefix", pa.string()),
        ("seller_city",            pa.string()),
        ("seller_state",           pa.string()),
        ("_loaded_at",             pa.timestamp("us")),
    ]),
    "geolocation": pa.schema([
        ("geolocation_zip_code_prefix", pa.string()),
        ("geolocation_lat",             pa.float64()),
        ("geolocation_lng",             pa.float64()),
        ("geolocation_city",            pa.string()),
        ("geolocation_state",           pa.string()),
        ("_loaded_at",                  pa.timestamp("us")),
    ]),
    "category_translation": pa.schema([
        ("product_category_name",         pa.string()),
        ("product_category_name_english", pa.string()),
        ("_loaded_at",                    pa.timestamp("us")),
    ]),
}

LOADERS = [
    ("olist_orders_dataset.csv",                  "orders"),
    ("olist_order_items_dataset.csv",             "order_items"),
    ("olist_customers_dataset.csv",               "customers"),
    ("olist_order_payments_dataset.csv",          "order_payments"),
    ("olist_order_reviews_dataset.csv",           "order_reviews"),
    ("olist_products_dataset.csv",                "products"),
    ("olist_sellers_dataset.csv",                 "sellers"),
    ("olist_geolocation_dataset.csv",             "geolocation"),
    ("product_category_name_translation.csv",     "category_translation"),
]

# ── Helpers ─────────────────────────────────────────────────────────────────
def get_s3():
    return boto3.client("s3", endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_KEY, aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1")

def get_trino():
    return connect(host=TRINO_HOST, port=TRINO_PORT,
                   user="admin", catalog="iceberg", schema="bronze",
                   request_timeout=60)

def run_trino(cur, sql):
    cur.execute(sql)
    try: return cur.fetchall()
    except: return []

def delete_minio_prefix(s3, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objs})
            deleted += len(objs)
    return deleted

def csv_to_pyarrow_table(csv_path, schema):
    """Lit le CSV et le convertit en PyArrow Table avec les bons types."""
    cols = [f.name for f in schema if f.name != "_loaded_at"]

    # Lire tout en string d'abord pour éviter les erreurs de parsing
    df = pd.read_csv(csv_path, dtype=str, low_memory=False, usecols=lambda c: c in cols)

    # Réordonner les colonnes selon le schéma
    available = [c for c in cols if c in df.columns]
    df = df[available]

    # Ajouter _loaded_at
    df["_loaded_at"] = pd.Timestamp.now()

    # Construire le PyArrow Table colonne par colonne avec cast
    arrays = []
    fields = []
    for field in schema:
        name = field.name
        if name not in df.columns:
            continue
        col = df[name]
        if pa.types.is_timestamp(field.type):
            arr = pa.array(pd.to_datetime(col, errors="coerce"), type=field.type)
        elif pa.types.is_floating(field.type):
            arr = pa.array(pd.to_numeric(col, errors="coerce"), type=field.type)
        elif pa.types.is_integer(field.type):
            num = pd.to_numeric(col, errors="coerce")
            arr = pa.array(num.where(num.notna(), None), type=field.type)
        else:
            arr = pa.array(col.where(col.notna(), None), type=pa.string())
            if field.type != pa.string():
                arr = arr.cast(field.type)
        arrays.append(arr)
        fields.append(field)

    return pa.table(arrays, schema=pa.schema(fields))

def upload_parquet(s3, arrow_table, table):
    """Écrit le Parquet et l'uploade dans MinIO."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    pq.write_table(
        arrow_table,
        tmp_path,
        compression="snappy",
        row_group_size=100_000,
    )
    size_mb = os.path.getsize(tmp_path) / 1024 / 1024

    # Supprimer l'ancien fichier s'il existe
    delete_minio_prefix(s3, f"bronze/{table}/data/")

    s3_key = f"bronze/{table}/data/part-00000.parquet"
    s3.upload_file(tmp_path, BUCKET, s3_key)
    os.unlink(tmp_path)

    return s3_key, size_mb

def register_iceberg_table(cur, table, schema):
    """
    Enregistre la table Iceberg dans Trino.
    Étapes :
      A. DROP TABLE IF EXISTS (pour repartir proprement)
      B. CREATE TABLE ... WITH (location=...) sur le dossier data/
         Trino Iceberg + iceberg.unique-table-location=false accepte un dossier
         non vide et scanne les fichiers Parquet présents.
    """
    location = f"s3://{BUCKET}/bronze/{table}/"

    # Construire la définition des colonnes depuis le schéma PyArrow
    TYPE_MAP = {
        pa.string():        "VARCHAR",
        pa.int32():         "INTEGER",
        pa.int64():         "BIGINT",
        pa.float32():       "REAL",
        pa.float64():       "DOUBLE",
    }
    col_defs = []
    for field in schema:
        sql_type = "TIMESTAMP(6)" if pa.types.is_timestamp(field.type) \
                   else TYPE_MAP.get(field.type, "VARCHAR")
        col_defs.append(f"    {field.name} {sql_type}")

    cols_sql = ",\n".join(col_defs)

    try:
        run_trino(cur, f"DROP TABLE IF EXISTS iceberg.bronze.{table}")
    except: pass

    create_sql = f"""
        CREATE TABLE iceberg.bronze.{table} (
        {cols_sql}
        ) WITH (
            format   = 'PARQUET',
            location = '{location}'
        )
    """
    run_trino(cur, create_sql)

def verify_count(cur, table):
    try:
        cur.execute(f"SELECT COUNT(*) FROM iceberg.bronze.{table}")
        return cur.fetchone()[0]
    except Exception as e:
        print(f"    ⚠  COUNT error: {e}")
        return -1

# ── Main ────────────────────────────────────────────────────────────────────
def load_one(s3, trino_cur, csv_file, table):
    csv_path = f"{DATA_DIR}/{csv_file}"
    schema   = SCHEMAS[table]

    if not os.path.exists(csv_path):
        print(f"  ⚠  Manquant : {csv_path}"); return 0

    t0 = time.time()

    # 1. CSV → PyArrow
    print(f"    → Lecture CSV...", end=" ", flush=True)
    arrow_table = csv_to_pyarrow_table(csv_path, schema)
    n = len(arrow_table)
    print(f"{n:,} lignes ({time.time()-t0:.1f}s)")

    # 2. Parquet → MinIO
    print(f"    → Écriture Parquet + upload MinIO...", end=" ", flush=True)
    t1 = time.time()
    s3_key, size_mb = upload_parquet(s3, arrow_table, table)
    print(f"{size_mb:.1f} Mo ({time.time()-t1:.1f}s)")

    # 3. Enregistrer dans Iceberg via Trino
    print(f"    → Enregistrement table Iceberg...", end=" ", flush=True)
    t2 = time.time()
    register_iceberg_table(trino_cur, table, schema)
    print(f"({time.time()-t2:.1f}s)")

    # 4. Vérifier
    count = verify_count(trino_cur, table)
    total_s = time.time() - t0

    if count > 0:
        print(f"    ✓ {count:,} lignes visibles dans bronze.{table} ({total_s:.1f}s total)")
    else:
        print(f"    ⚠  0 lignes — Trino redémarre parfois lentement, réessayez dans 10s")
        print(f"       Vérification manuelle : docker exec trino trino --execute \"SELECT COUNT(*) FROM iceberg.bronze.{table}\"")

    return n


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", help="Charger une seule table (ex: orders)")
    parser.add_argument("--reset", action="store_true",
                        help="Supprimer toutes les tables Bronze avant de charger")
    args = parser.parse_args()

    s3   = get_s3()
    conn = get_trino()
    cur  = conn.cursor()

    if args.reset:
        print("── Reset Bronze ──")
        tables = [t for _, t in LOADERS]
        for t in tables:
            try:
                run_trino(cur, f"DROP TABLE IF EXISTS iceberg.bronze.{t}")
                print(f"  ✓ DROP bronze.{t}")
            except: pass
            delete_minio_prefix(s3, f"bronze/{t}/")
        print()

    loaders = [(f, t) for f, t in LOADERS
               if not args.table or t == args.table]

    print(f"✓ Chargement via PyArrow + Parquet ({len(loaders)} table(s))\n")

    total_rows, start = 0, time.time()
    for csv_file, table in loaders:
        print(f"── {table}")
        n = load_one(s3, cur, csv_file, table)
        total_rows += n
        print()

    elapsed = time.time() - start
    m, s = divmod(int(elapsed), 60)
    print(f"✓ {total_rows:,} lignes traitées en {m}m{s:02d}s")
    print("  Prochaine étape : ./run_pipeline.sh")

if __name__ == "__main__":
    main()