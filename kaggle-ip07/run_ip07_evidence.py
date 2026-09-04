"""Run a real vLLM server on Kaggle and export the IP07 identity evidence."""

from __future__ import annotations

import json
import subprocess
import sys
import time
import traceback
from pathlib import Path

import requests

MODEL = "Qwen/Qwen3-4B-Instruct-2507"
BASE_URL = "http://127.0.0.1:8000"
EVIDENCE_PATH = Path("ip07-vllm-identity.json")
LOG_PATH = Path("vllm-server.log")


def request(path: str) -> requests.Response:
    return requests.get(f"{BASE_URL}{path}", timeout=15)


def wait_for_server(process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 20 * 60
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"vLLM exited with code {process.returncode}")
        try:
            if request("/health").status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    raise TimeoutError("vLLM did not become healthy within 20 minutes")


def metric_names(body: str) -> list[str]:
    names = {
        line.split(" ", 1)[0].split("{", 1)[0]
        for line in body.splitlines()
        if line.startswith("vllm:")
    }
    return sorted(names)


def main() -> None:
    install = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--no-cache-dir", "vllm==0.8.5"],
        capture_output=True,
        text=True,
        check=False,
    )
    Path("pip-install.log").write_text(install.stdout + install.stderr, encoding="utf-8")
    install.check_returncode()
    command = [
        "vllm",
        "serve",
        MODEL,
        "--host",
        "0.0.0.0",
        "--port",
        "8000",
        "--dtype",
        "half",
        "--max-model-len",
        "4096",
        "--gpu-memory-utilization",
        "0.85",
        "--enforce-eager",
    ]
    with LOG_PATH.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, text=True)
        try:
            wait_for_server(process)
            version = request("/version")
            models = request("/v1/models")
            metrics = request("/metrics")
            version.raise_for_status()
            models.raise_for_status()
            metrics.raise_for_status()
            metric_list = metric_names(metrics.text)
            payload = {
                "reachable": True,
                "server": "Kaggle NVIDIA Tesla T4",
                "version": version.json(),
                "served_models": [item["id"] for item in models.json().get("data", [])],
                "vllm_metric_names": metric_list,
                "vllm_metric_count": len(metric_list),
                "is_real_vllm": bool(metric_list),
            }
            EVIDENCE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            print(EVIDENCE_PATH.read_text(encoding="utf-8"))
        finally:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        Path("kernel-error.txt").write_text(traceback.format_exc(), encoding="utf-8")
        raise
