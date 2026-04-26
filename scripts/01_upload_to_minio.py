


import boto3, os, sys, glob
from botocore.exceptions import ClientError
from cadwyn import endpoint
from more_itertools import bucket

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS   = "admin"
MINIO_SECRET   = "password123"
BUCKET         = "lakehouse-bronze"
DATA_DIR       = "./data/olist"



def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1",
    )


    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"Bucket '{BUCKET}' accessible.")
    except ClientError as e:
        print(f"Bucket '{BUCKET}' introuvable — lancez d'abord ./start.sh")
        sys.exit(1)
    
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    if not csv_files:
        print(f"Aucun fichier CSV trouvé dans '{DATA_DIR}'.")
        sys.exit(1)
    
    print(f"\n{len(csv_files)} fichiers CSV → upload vers MinIO...\n")

    for path in csv_files:
        filename = os.path.basename(path)
        size_mb = os.path.getsize(path)  / 1024 / 1024
        key = f"raw/{filename}"
        print(f" {filename:<55} ({size_mb:.1f} Mo)", end=" ", flush=True)
        s3.upload_file(path, BUCKET, key)
        print(" SUCCESS")

    print(f"\n Upload terminé dans s3://{BUCKET}/raw/")
    print("  Vérifiez dans MinIO : http://localhost:9001")


if __name__ == "__main__":
    main()


