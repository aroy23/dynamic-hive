#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

BEELINE="docker exec hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
ok()    { echo -e "${GREEN}[ OK ]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; }

info "Step 1: Building Hive hook JAR..."
if command -v mvn &>/dev/null; then
    (cd hive/hook && mvn clean package -q -DskipTests)
    ok "Hook JAR built: hive/hook/target/query-access-hook-1.0.jar"
else
    warn "Maven not found. Checking for pre-built JAR..."
    if [ -f hive/hook/target/query-access-hook-1.0.jar ]; then
        ok "Using pre-built JAR."
    else
        fail "No Maven and no pre-built JAR. Install Maven or build the JAR manually."
        exit 1
    fi
fi

info "Step 2: Starting Docker cluster..."
docker compose up -d
ok "Docker containers started."

info "Step 3: Waiting for services to be ready..."

wait_for_container() {
    local container="$1"
    local check_cmd="$2"
    local max_attempts="${3:-60}"
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if docker exec "$container" bash -c "$check_cmd" &>/dev/null; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    return 1
}

info "  Waiting for HDFS NameNode..."
if wait_for_container namenode "hdfs dfs -ls /" 60; then
    ok "  HDFS NameNode ready."
else
    fail "  HDFS NameNode did not become ready in time."
    exit 1
fi

info "  Waiting for HiveServer2..."
if wait_for_container hive-server "/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e 'SELECT 1'" 90; then
    ok "  HiveServer2 ready."
else
    fail "  HiveServer2 did not become ready in time."
    exit 1
fi

info "Step 4: Creating sample Hive tables..."

$BEELINE -e "
CREATE TABLE IF NOT EXISTS orders (
    order_id INT,
    customer_id INT,
    amount DOUBLE,
    order_date STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
"

$BEELINE -e "
CREATE TABLE IF NOT EXISTS archive_2020 (
    record_id INT,
    data STRING,
    created_date STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
"

$BEELINE -e "
INSERT INTO orders VALUES
(1, 101, 29.99, '2026-03-01'),
(2, 102, 49.50, '2026-03-02'),
(3, 103, 15.00, '2026-03-03'),
(4, 101, 89.99, '2026-03-04'),
(5, 104, 12.50, '2026-03-04');
"

$BEELINE -e "
INSERT INTO archive_2020 VALUES
(1, 'old record A', '2020-01-15'),
(2, 'old record B', '2020-06-20'),
(3, 'old record C', '2020-12-01');
"

ok "Sample tables created and populated."

info "Step 5: Checking initial replication factors..."
echo ""

show_replication() {
    local label="$1"
    local path="$2"
    info "  Replication for $label ($path):"
    docker exec namenode hdfs dfs -ls "$path" 2>/dev/null | grep -v "^Found" | head -5 || true
    echo ""
}

show_replication "orders" "/user/hive/warehouse/orders"
show_replication "archive_2020" "/user/hive/warehouse/archive_2020"

info "Step 6: Simulating hot access pattern..."
info "  Running 60 queries against 'orders' (hot table)..."

for i in $(seq 1 60); do
    $BEELINE -e "SELECT COUNT(*) FROM orders;" &>/dev/null &
    if (( i % 10 == 0 )); then
        wait
        info "    ...sent $i queries"
    fi
done
wait

info "  Running 0 queries against 'archive_2020' (cold table)."
ok "Access simulation complete."

info "Step 7: Access log contents:"
echo ""
if [ -f logs/access_log.jsonl ]; then
    LINE_COUNT=$(wc -l < logs/access_log.jsonl | tr -d ' ')
    info "  $LINE_COUNT entries in access_log.jsonl"
    info "  Last 5 entries:"
    tail -5 logs/access_log.jsonl | while read -r line; do
        echo "    $line"
    done
else
    warn "  No access log found. The hook may not be writing yet."
fi
echo ""

info "Step 8: Computing heat scores..."
echo ""
python3 scripts/heat_tracker.py scripts/config.yaml
echo ""

info "Step 9: Adjusting replication factors based on heat..."
echo ""
python3 scripts/replication_manager.py scripts/config.yaml
echo ""

info "Step 10: Final replication factors:"
echo ""
show_replication "orders" "/user/hive/warehouse/orders"
show_replication "archive_2020" "/user/hive/warehouse/archive_2020"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Demo complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Summary:"
echo "  - 'orders' table: queried 60 times -> should be HOT (rep=3)"
echo "  - 'archive_2020' table: queried 0 times -> should be COLD (rep=1)"
echo ""
echo "To run the daemon continuously:"
echo "  cd scripts && python3 run_daemon.py config.yaml"
echo ""
echo "To stop the cluster:"
echo "  docker compose down"
