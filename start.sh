#!/usr/bin/env bash
set -e

GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
header() { echo -e "\n${CYAN}━━━ $1 ━━━${NC}"; }
ok()     { echo -e "${GREEN}✓ $1${NC}"; }
info()   { echo -e "${YELLOW}→ $1${NC}"; }
err()    { echo -e "${RED}✗ $1${NC}"; exit 1; }

header "Data Lakehouse — Démarrage"
command -v docker >/dev/null 2>&1 || err "Docker non trouvé"
docker compose version >/dev/null 2>&1 || err "docker compose v2 non trouvé"
ok "Docker disponible"

# ── 1. Stockage ──────────────────────────────────────────────
header "1/4 — MinIO + Nessie + PostgreSQL"
docker compose up -d minio postgres nessie
info "Attente santé des services..."
docker compose up minio-init
ok "Stockage et buckets prêts"

# ── 2. Trino ─────────────────────────────────────────────────
header "2/4 — Trino"
docker compose up -d trino
info "Attente Trino (peut prendre 60s)..."
TRIES=0
until curl -sf http://localhost:8080/v1/info >/dev/null 2>&1; do
  TRIES=$((TRIES+1))
  [ $TRIES -ge 24 ] && {
    echo ""
    err "Trino n'a pas démarré. Diagnostic : docker logs trino --tail 30"
  }
  sleep 5; printf "."
done
echo ""
ok "Trino disponible — http://localhost:8080"

# ── 3. Airflow ───────────────────────────────────────────────
header "3/4 — Airflow"
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler
ok "Airflow démarré — http://localhost:8081"

# ── 4. dbt ───────────────────────────────────────────────────
header "4/4 — dbt (build image pip install)"
docker compose build dbt
docker compose up -d dbt
ok "Container dbt prêt"

# ── Schémas Iceberg (via docker exec) ────────────────────────
header "Initialisation Iceberg"
info "Création des schémas Bronze / Silver / Gold..."
docker exec trino trino --execute \
  "CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location = 's3://nessie-warehouse/bronze/');
   CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location = 's3://nessie-warehouse/silver/');
   CREATE SCHEMA IF NOT EXISTS iceberg.gold   WITH (location = 's3://nessie-warehouse/gold/');"
ok "Schémas créés"

info "Création de la table orders_raw..."
docker exec trino trino --execute \
  "CREATE TABLE IF NOT EXISTS iceberg.bronze.orders_raw (
     order_id     VARCHAR,
     user_id      VARCHAR,
     total_amount DOUBLE,
     status       VARCHAR,
     created_at   TIMESTAMP(6),
     _loaded_at   TIMESTAMP(6)
   ) WITH (format = 'PARQUET', location = 's3://nessie-warehouse/bronze/orders_raw/');"
ok "Table orders_raw créée"

# ── Résumé ───────────────────────────────────────────────────
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  Stack prête !${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  MinIO Console  →  ${CYAN}http://localhost:9001${NC}   admin / password123"
echo -e "  Nessie UI      →  ${CYAN}http://localhost:19120/ui${NC}"
echo -e "  Trino UI       →  ${CYAN}http://localhost:8080${NC}"
echo -e "  Airflow UI     →  ${CYAN}http://localhost:8081${NC}   admin / admin"
echo ""