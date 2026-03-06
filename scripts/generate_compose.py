#!/usr/bin/env python3

import sys
from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def generate_compose(num_datanodes=3):
    services = {}

    services["namenode"] = {
        "image": "bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8",
        "container_name": "namenode",
        "restart": "always",
        "ports": ["9870:9870", "9000:9000"],
        "volumes": ["hadoop_namenode:/hadoop/dfs/name", "./logs:/opt/logs"],
        "environment": ["CLUSTER_NAME=hive-cluster"],
        "env_file": ["./hadoop-hive.env"],
    }

    for i in range(1, num_datanodes + 1):
        host_port = 9864 + (i - 1)
        services[f"datanode{i}"] = {
            "image": "bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8",
            "container_name": f"datanode{i}",
            "restart": "always",
            "ports": [f"{host_port}:9864"],
            "volumes": [f"hadoop_datanode{i}:/hadoop/dfs/data"],
            "environment": {"SERVICE_PRECONDITION": "namenode:9870"},
            "env_file": ["./hadoop-hive.env"],
        }

    services["hive-metastore-postgresql"] = {
        "image": "bde2020/hive-metastore-postgresql:2.3.0",
        "container_name": "hive-metastore-postgresql",
        "restart": "always",
        "volumes": ["hive_metastore_postgresql:/var/lib/postgresql/data"],
    }

    services["hive-metastore"] = {
        "image": "bde2020/hive:2.3.2-postgresql-metastore",
        "container_name": "hive-metastore",
        "restart": "always",
        "env_file": ["./hadoop-hive.env"],
        "command": "/opt/hive/bin/hive --service metastore",
        "environment": {
            "SERVICE_PRECONDITION": "namenode:9870 hive-metastore-postgresql:5432",
        },
        "ports": ["9083:9083"],
    }

    services["hive-server"] = {
        "image": "bde2020/hive:2.3.2-postgresql-metastore",
        "container_name": "hive-server",
        "restart": "always",
        "env_file": ["./hadoop-hive.env"],
        "environment": {
            "HIVE_CORE_CONF_javax_jdo_option_ConnectionURL": "jdbc:postgresql://hive-metastore-postgresql/metastore",
            "SERVICE_PRECONDITION": "hive-metastore:9083",
        },
        "ports": ["10000:10000", "10002:10002"],
        "volumes": [
            "./logs:/opt/logs",
            "./hive/hook/target/query-access-hook-1.0.jar:/opt/hive/lib/query-access-hook-1.0.jar",
            "./hive/conf/hive-site-override.xml:/opt/hive/conf/hive-site.xml",
        ],
    }

    volumes = {"hadoop_namenode": None}
    for i in range(1, num_datanodes + 1):
        volumes[f"hadoop_datanode{i}"] = None
    volumes["hive_metastore_postgresql"] = None

    return yaml.dump(
        {"version": "3", "services": services, "volumes": volumes},
        default_flow_style=False,
        sort_keys=False,
    )


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else str(PROJECT_ROOT / "config.yaml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    num_datanodes = config.get("num_datanodes", 3)
    if num_datanodes < 1:
        print(f"Error: num_datanodes must be >= 1, got {num_datanodes}", file=sys.stderr)
        sys.exit(1)

    output_path = PROJECT_ROOT / "docker-compose.yml"
    output_path.write_text(generate_compose(num_datanodes))
    print(f"Generated {output_path} with {num_datanodes} datanode(s).")


if __name__ == "__main__":
    main()
