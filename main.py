from __future__ import annotations

import argparse
import queue
import threading
import time
from dataclasses import asdict
from typing import Any

from config import AppConfig
from gui import DashboardState, create_app


class WorkerPool:
    def __init__(self, config: AppConfig, state: DashboardState) -> None:
        self.config = config
        self.state = state
        self.jobs: queue.Queue[dict[str, Any]] = queue.Queue()
        self.stop_event = threading.Event()
        self.threads: list[threading.Thread] = []

    def start(self) -> None:
        self.state.status = "running"
        self.state.workers = self.config.worker_threads
        for index in range(self.config.worker_threads):
            thread = threading.Thread(target=self.worker_loop, name=f"worker-{index+1}", daemon=True)
            thread.start()
            self.threads.append(thread)

    def submit(self, job: dict[str, Any]) -> None:
        self.jobs.put(job)

    def worker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                job = self.jobs.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self.state.active_jobs += 1
                time.sleep(0.1)
                _ = job
            except Exception as exc:
                self.state.last_error = str(exc)
            finally:
                self.state.active_jobs = max(0, self.state.active_jobs - 1)
                self.jobs.task_done()

    def stop(self) -> None:
        self.stop_event.set()


def run_flask(app, host: str, port: int, debug: bool) -> None:
    app.run(host=host, port=port, debug=debug, use_reloader=False, threaded=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Flask dashboard and worker threads")
    parser.add_argument("--config", default=".", help="Project root containing config.json and .env")
    args = parser.parse_args()

    config = AppConfig.load(args.config)
    state = DashboardState(status="starting", workers=config.worker_threads, active_jobs=0)
    app = create_app(config, state=state)
    workers = WorkerPool(config, state)
    workers.start()

    flask_thread = threading.Thread(
        target=run_flask,
        name="flask-server",
        args=(app, config.flask_host, config.flask_port, config.debug),
        daemon=True,
    )
    flask_thread.start()

    try:
        while flask_thread.is_alive():
            time.sleep(1.0)
    except KeyboardInterrupt:
        state.status = "stopping"
        workers.stop()


if __name__ == "__main__":
    main()
