"""
scripts/00_reset_bronze.py
─────────────────────────────────────────────────────────────────────────────
Supprime tous les fichiers Bronze dans MinIO et les tables Iceberg Bronze dans
Trino. À exécuter UNIQUEMENT pour repartir de zéro.

Usage : python scripts/00_reset_bronze.py
"""
import boto3
from trino.dbapi import connect

MINIO_URL    = "http://localhost:9000"
MINIO_KEY    = "admin"
MINIO_SECRET = "password123"
BUCKET       = "nessie-warehouse"
TABLES       = ["orders","order_items","customers","order_payments",
                "order_reviews","products","sellers","geolocation",
                "category_translation"]

def main():
    s3 = boto3.client("s3", endpoint_url=MINIO_URL,
                      aws_access_key_id=MINIO_KEY,
                      aws_secret_access_key=MINIO_SECRET,
                      region_name="us-east-1")

    conn = connect(host="localhost", port=8080, user="admin", catalog="iceberg")
    cur  = conn.cursor()

    print("── Suppression tables Trino ──")
    for t in TABLES:
        try:
            cur.execute(f"DROP TABLE IF EXISTS iceberg.bronze.{t}")
            cur.fetchall()
            print(f"  ✓ DROP iceberg.bronze.{t}")
        except Exception as e:
            print(f"  ⚠  {t}: {e}")

    print("\n── Suppression fichiers MinIO bronze/ ──")
    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix="bronze/"):
        objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objects})
            deleted += len(objects)
    print(f"  ✓ {deleted} fichiers supprimés de s3://{BUCKET}/bronze/")

    print("\n✓ Reset terminé — relancez dans l'ordre :")
    print("  1. py scripts\\02_create_bronze_tables.py")
    print("  2. py scripts\\03_load_bronze.py")

if __name__ == "__main__":
    main()