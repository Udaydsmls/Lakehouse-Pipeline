import multiprocessing
import signal
import sys
import time

import cart_producer
import clickstream_producer
import inventory_producer
import order_producer
import search_producer
import user_producer
from config import ProducerConfig


def _run_producer(module, config: ProducerConfig):
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    module.run(config)


def main():
    config = ProducerConfig()

    producers = [
        ("clickstream", clickstream_producer),
        ("cart", cart_producer),
        ("orders", order_producer),
        ("users", user_producer),
        ("search", search_producer),
        ("inventory", inventory_producer),
    ]

    processes: list[multiprocessing.Process] = []
    for name, module in producers:
        p = multiprocessing.Process(
            target=_run_producer,
            args=(module, config),
            name=name,
            daemon=True,
        )
        p.start()
        processes.append(p)
        print(f"[run_all] started {name} producer (pid={p.pid})", file=sys.stderr)

    def _shutdown(signum, frame):
        print("\n[run_all] received signal, shutting down producers...", file=sys.stderr)
        for p in processes:
            if p.is_alive():
                p.terminate()
        for p in processes:
            p.join(timeout=10)
            if p.is_alive():
                p.kill()
        print("[run_all] all producers stopped", file=sys.stderr)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    while True:
        for p in processes:
            if not p.is_alive():
                print(f"[run_all] producer '{p.name}' exited with code {p.exitcode}", file=sys.stderr)
        time.sleep(5)


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")
    main()
