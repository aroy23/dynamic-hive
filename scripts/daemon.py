import sys
import time
import signal
import logging
import yaml

from heat_tracker import get_heat_report
from replication_manager import adjust_replications

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("daemon")

running = True


def handle_signal(signum, frame):
    global running
    log.info("Received signal %d, shutting down...", signum)
    running = False


def main(config_path="config.yaml"):
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    interval = config.get("poll_interval_seconds", 30)
    log.info("Dynamic Replication Daemon started (interval=%ds, datanodes=%d, heat_per_replica=%d)",
             interval, config.get("num_datanodes", 3), config.get("heat_per_replica", 10))

    cycle = 0
    while running:
        cycle += 1
        log.info("=" * 60)
        log.info("Cycle %d", cycle)

        try:
            report = get_heat_report(config_path)
            if report:
                for entry in report:
                    log.info("  %-35s heat=%-8.2f desired_rep=%d",
                             entry["table"], entry["heat"], entry["desired_replication"])
            else:
                log.info("No table accesses found in log.")

            changes = adjust_replications(config_path)
            if changes:
                log.info("Applied %d change(s):", len(changes))
                for c in changes:
                    log.info("  %s: %d -> %d (heat=%.2f)",
                             c["table"], c["old_replication"], c["new_replication"], c["heat"])
            else:
                log.info("No replication changes needed.")

        except Exception as e:
            log.exception("Error during cycle %d: %s", cycle, e)

        if running:
            log.info("Sleeping %ds...", interval)
            for _ in range(interval):
                if not running:
                    break
                time.sleep(1)

    log.info("Daemon stopped.")


if __name__ == "__main__":
    cfg = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    main(cfg)
