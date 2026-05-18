"""
scripts/02_create_bronze_tables.py
Crée les schémas Iceberg et les 9 tables Bronze dans Trino.

Usage : python scripts/02_create_bronze_tables.py
"""

from trino.dbapi import connect
import sys

TRINO_HOST = "localhost"
TRINO_PORT = 8080
WAREHOUSE  = "s3://nessie-warehouse"

SCHEMAS = {
    "bronze": f"{WAREHOUSE}/bronze/",
    "silver": f"{WAREHOUSE}/silver/",
    "gold":   f"{WAREHOUSE}/gold/",
}

TABLES = [
    ("bronze", "orders", """
        order_id                        VARCHAR,
        customer_id                     VARCHAR,
        order_status                    VARCHAR,
        order_purchase_timestamp        VARCHAR,
        order_approved_at               VARCHAR,
        order_delivered_carrier_date    VARCHAR,
        order_delivered_customer_date   VARCHAR,
        order_estimated_delivery_date   VARCHAR,
        _loaded_at                      TIMESTAMP(6)
    """, f"{WAREHOUSE}/bronze/orders/"),

    ("bronze", "order_items", """
        order_id            VARCHAR,
        order_item_id       INTEGER,
        product_id          VARCHAR,
        seller_id           VARCHAR,
        shipping_limit_date VARCHAR,
        price               DOUBLE,
        freight_value       DOUBLE,
        _loaded_at          TIMESTAMP(6)
    """, f"{WAREHOUSE}/bronze/order_items/"),

    ("bronze", "customers", """
        customer_id                 VARCHAR,
        customer_unique_id          VARCHAR,
        customer_zip_code_prefix    VARCHAR,
        customer_city               VARCHAR,
        customer_state              VARCHAR,
        _loaded_at                  TIMESTAMP(6)
    """, f"{WAREHOUSE}/bronze/customers/"),

    ("bronze", "order_payments", """
        order_id                VARCHAR,
        payment_sequential      INTEGER,
        payment_type            VARCHAR,
        payment_installments    INTEGER,
        payment_value           DOUBLE,
        _loaded_at              TIMESTAMP(6)
    """, f"{WAREHOUSE}/bronze/order_payments/"),

    ("bronze", "order_reviews", """
        review_id               VARCHAR,
        order_id                VARCHAR,
        review_score            INTEGER,
        review_comment_title    VARCHAR,
        review_comment_message  VARCHAR,
        review_creation_date    VARCHAR,
        review_answer_timestamp VARCHAR,
        _loaded_at              TIMESTAMP(6)
    """, f"{WAREHOUSE}/bronze/order_reviews/"),

    ("bronze", "products", """
        product_id                  VARCHAR,
        product_category_name       VARCHAR,
        product_name_length         DOUBLE,
        product_description_length  DOUBLE,
        product_photos_qty          DOUBLE,
        product_weight_g            DOUBLE,
        product_length_cm           DOUBLE,
        product_height_cm           DOUBLE,
        product_width_cm            DOUBLE,
        _loaded_at                  TIMESTAMP(6)
    """, f"{WAREHOUSE}/bronze/products/"),

    ("bronze", "sellers", """
        seller_id               VARCHAR,
        seller_zip_code_prefix  VARCHAR,
        seller_city             VARCHAR,
        seller_state            VARCHAR,
        _loaded_at              TIMESTAMP(6)
    """, f"{WAREHOUSE}/bronze/sellers/"),

    ("bronze", "geolocation", """
        geolocation_zip_code_prefix VARCHAR,
        geolocation_lat             DOUBLE,
        geolocation_lng             DOUBLE,
        geolocation_city            VARCHAR,
        geolocation_state           VARCHAR,
        _loaded_at                  TIMESTAMP(6)
    """, f"{WAREHOUSE}/bronze/geolocation/"),

    ("bronze", "category_translation", """
        product_category_name           VARCHAR,
        product_category_name_english   VARCHAR,
        _loaded_at                      TIMESTAMP(6)
    """, f"{WAREHOUSE}/bronze/category_translation/"),
]

def run(cur, sql, label=""):
    try:
        cur.execute(sql)
        try: cur.fetchall()
        except: pass
        return True
    except Exception as e:
        print(f"{label}: {e}")
        return False

def main():
    print("Connexion à Trino...")
    try:
        conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="admin", catalog="iceberg")
        cur  = conn.cursor()
        cur.execute("SELECT 1"); cur.fetchall()
        print("Trino accessible\n")
    except Exception as e:
        print(f"{e}\n  Vérifiez que ./start.sh est terminé.")
        sys.exit(1)

    print("── Schémas Iceberg ──")
    for schema, location in SCHEMAS.items():
        ok = run(cur, f"CREATE SCHEMA IF NOT EXISTS iceberg.{schema} WITH (location = '{location}')", schema)
        print(f"  {'✓' if ok else '✗'} iceberg.{schema}")

    print("\n── Tables Bronze ──")
    for schema, table, cols, location in TABLES:
        sql = f"CREATE TABLE IF NOT EXISTS iceberg.{schema}.{table} ({cols}) WITH (format='PARQUET', location='{location}')"
        ok = run(cur, sql, f"{schema}.{table}")
        print(f"  {'✓' if ok else '✗'} iceberg.{schema}.{table}")

    print("\n✓ Tables prêtes → python scripts/03_load_bronze.py")

if __name__ == "__main__":
    main()