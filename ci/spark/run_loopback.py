#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-License-Identifier: Apache-2.0
"""Run a Spark raw-ibverbs CI profile from the DAQIRI checkout root."""

from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import uuid

import yaml

from scripts.daqiri_config import RawPairSpec, generate_raw_pair, render_document


def read(path: Path, default: str = "") -> str:
    try:
        return path.read_text().strip()
    except OSError:
        return default


def discover(sysfs: Path = Path("/sys/class/net")) -> list[dict]:
    ports = []
    for net in sorted(sysfs.iterdir()):
        device = (net / "device").resolve()
        if read(device / "vendor") != "0x15b3":
            continue
        if (device / "driver").resolve().name != "mlx5_core":
            continue
        rdma = sorted((device / "infiniband").glob("*"))
        if not rdma:
            continue
        layers = [read(p) for p in rdma[0].glob("ports/*/link_layer")]
        ports.append(dict(netdev=net.name, bdf=device.name, rdma=rdma[0].name,
                          physical_port=read(net / "phys_port_name"),
                          mac=read(net / "address"), mtu=int(read(net / "mtu", "0")),
                          carrier=read(net / "carrier", "0") == "1",
                          ethernet="Ethernet" in layers))
    return ports


def select_ports(ports: list[dict], mode: str, tx_name: str | None,
                 rx_name: str | None) -> tuple[dict, dict]:
    eligible = [p for p in ports if p["ethernet"] and (mode == "hardware" or p["carrier"])]
    if tx_name:
        eligible_tx = [p for p in eligible if tx_name in (p["netdev"], p["bdf"])]
    else:
        eligible_tx = eligible
    if not eligible_tx:
        raise ValueError("No eligible mlx5 Ethernet TX port. Check enumeration, drivers and cables.")
    tx = eligible_tx[0]
    if mode == "hardware":
        if rx_name and rx_name not in (tx["netdev"], tx["bdf"]):
            raise ValueError("Hardware loopback requires the same TX/RX port")
        return tx, tx
    if not tx["physical_port"]:
        raise ValueError("phys_port_name is required to prove distinct physical ports")
    candidates = [p for p in eligible if p["physical_port"]
                  and p["physical_port"] != tx["physical_port"] and p["mac"] != tx["mac"]]
    if rx_name:
        candidates = [p for p in candidates if rx_name in (p["netdev"], p["bdf"])]
    if not candidates:
        raise ValueError("Cabled mode needs two linked, distinct physical ports (p0 and p1)")
    return tx, candidates[0]


def make_config(mode: str, tx: dict, rx: dict, cores: list[int]) -> dict:
    if len(cores) < 5 or len(set(cores)) != len(cores):
        raise ValueError("Provide at least five distinct allowed CPU cores")
    document = generate_raw_pair(RawPairSpec(
        tx_address=tx["bdf"], rx_address=rx["bdf"], master_core=cores[0],
        tx_queue_cores=(cores[1],), rx_queue_cores=(cores[2],),
        tx_worker_cores=(cores[3],), rx_worker_cores=(cores[4],),
        eth_src_addr=tx["mac"], eth_dst_addr=rx["mac"],
        ip_src_addr="192.0.2.1", ip_dst_addr="192.0.2.2", engine="ibverbs",
        memory_kind="host_pinned", num_bufs=8192, batch_size=256,
        payload_size=8000, header_size=64, buffer_size=8192))
    cfg = document["daqiri"]["cfg"]
    if mode == "hardware":
        tx_intf, rx_intf = cfg["interfaces"]
        tx_intf["rx"] = rx_intf["rx"]
        cfg["interfaces"] = [tx_intf]
        cfg["loopback"] = "hw"
        document["bench_rx"][0]["interface_name"] = tx_intf["name"]
    return document


def stats_errors(log: str) -> list[str]:
    errors = []
    for direction in ("TX", "RX"):
        counts = re.findall(rf"{direction} complete: interface=\S+ queue=0 packets=(\d+)", log)
        if not counts or int(counts[-1]) == 0:
            errors.append(f"Missing or zero {direction} application packet count")
    for field in ("cqe_errors", "app_ring_full_drops", "handoff_full_drops"):
        values = re.findall(rf"\b{field}=(\d+)", log)
        if not values:
            errors.append(f"Missing required queue counter {field}")
        elif any(int(n) for n in values):
            errors.append(f"Nonzero {field}")
    if re.search(r"\[(?:error|critical)\]|NO_FREE_(?:BURST|PACKET)_BUFFERS", log, re.I):
        errors.append("DAQIRI error or buffer exhaustion in benchmark log")
    return errors


def capture(command: list[str], path: Path, *, timeout: int = 60) -> str:
    # Write directly so partial output survives timeout or cancellation.
    with path.open("w") as handle:
        result = subprocess.run(command, stdout=handle, stderr=subprocess.STDOUT,
                                text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}); see {path}")
    return path.read_text()


def counters(netdev: str, probe: list[str]) -> dict[str, int]:
    result = subprocess.run(probe + ["ethtool", "-S", netdev], capture_output=True, text=True,
                            timeout=15, check=True)
    return {key: int(value) for key, value in re.findall(r"^\s*(\w+):\s*(\d+)\s*$",
                                                       result.stdout, re.M)}


def path_errors(mode: str, tx: dict, rx: dict, before: dict, after: dict,
                strict_physical: bool = False) -> list[str]:
    checks = [(tx["netdev"], "tx_packets_phy"), (rx["netdev"], "rx_packets_phy")]
    errors = []
    for netdev, key in checks:
        if key not in before[netdev] or key not in after[netdev]:
            errors.append(f"Missing physical counter {netdev}/{key}")
            continue
        delta = after[netdev][key] - before[netdev][key]
        if (mode == "cabled" and delta <= 0) or (mode == "hardware" and strict_physical and delta != 0):
            errors.append(f"Unexpected {netdev}/{key} delta {delta} for {mode} loopback")
    if mode == "hardware":
        key = "vport_loopback_packets"
        netdev = tx["netdev"]
        if key not in before[netdev] or key not in after[netdev]:
            errors.append("Missing vport_loopback_packets counter")
        elif after[netdev][key] <= before[netdev][key]:
            errors.append("No internal loopback packets observed")
    return errors


def run(args: argparse.Namespace) -> None:
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    summary = {"schema_version": 1, "mode": args.mode, "seconds": args.seconds,
               "status": "running", "profile": str(args.profile.resolve()),
               "strict_physical_counters": args.strict_physical_counters}
    try:
        revision = subprocess.run(
            ["git", "-C", str(Path(__file__).resolve().parents[2]), "rev-parse", "HEAD"],
            capture_output=True, text=True, check=True, timeout=15)
        summary["source_revision"] = revision.stdout.strip()
        ports = discover()
        (out / "ports.json").write_text(json.dumps(ports, indent=2))
        tx, rx = select_ports(ports, args.mode, args.tx, args.rx)
        allowed = os.sched_getaffinity(0)
        cores = args.cores or sorted(allowed, key=lambda c: (
            -int(read(Path(f"/sys/devices/system/cpu/cpufreq/policy{c}/cpuinfo_max_freq"), "0")), c))[:5]
        if not set(cores) <= allowed:
            raise ValueError("Selected cores are outside this process's CPU affinity")
        document = make_config(args.mode, tx, rx, cores)
        (out / "config.yaml").write_text(render_document(document))
        summary.update(tx=tx, rx=rx, cores=cores, image=args.image,
                       target_gbps=args.target_gbps, memory_kind="host_pinned")
        print(json.dumps(summary, indent=2), flush=True)
        if args.prepare_only:
            summary["status"] = "prepared"
            return

        # Advisory host lock: use the same path for every runner using this Spark.
        with open("/tmp/daqiri-spark-ci.lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            execute(args, out, tx, rx, cores, summary)
    except BaseException as exc:
        summary["status"] = "failed"
        summary["failure"] = str(exc)
        raise
    finally:
        (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(f"{summary['status']}: {out / 'summary.json'}", flush=True)


def remove_container(name: str) -> None:
    result = subprocess.run(["docker", "rm", "-f", name], capture_output=True,
                            text=True, timeout=30)
    if result.returncode and "No such container:" not in result.stderr:
        raise RuntimeError(f"Cannot remove container {name}: {result.stderr.strip()}")


def stop_monitor(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait(timeout=5)


def execute(args: argparse.Namespace, out: Path, tx: dict, rx: dict,
            cores: list[int], summary: dict) -> None:
    selected = sorted({tx["netdev"], rx["netdev"]})
    original_mtus = {name: int(read(Path("/sys/class/net") / name / "mtu")) for name in selected}
    name = "daqiri-spark-ci-" + uuid.uuid4().hex[:12]
    docker = ["docker", "run", "--rm", "--privileged", "--network=host", "--gpus=all",
              "--cpuset-cpus=" + ",".join(map(str, cores)), "--ulimit", "memlock=-1:-1",
              "--mount", f"type=bind,src={out},dst=/results,readonly"]
    # Host namespaces, container tools: the job account needs Docker access,
    # not a passwordless root Python interpreter on the host.
    probe = ["docker", "run", "--rm", "--privileged", "--network=host",
             "--entrypoint", "/usr/bin/env", args.image]
    monitors = []
    monitor_names = []
    handles = []
    try:
        capture(["docker", "image", "inspect", args.image], out / "image.json")
        capture(probe + ["sh", "-ec", "command -v ethtool; command -v ip; command -v mlnx_perf"],
                out / "runtime-tools.log")
        engines = capture(docker + [args.image, "/opt/daqiri/bin/daqiri_config_validate",
                                    "--list-engines"], out / "engines.log")
        if "ibverbs" not in engines.split():
            raise RuntimeError("Container validator does not list ibverbs")
        capture(docker + [args.image, "/opt/daqiri/bin/daqiri_config_validate",
                          "/results/config.yaml"], out / "validation.log")
        capture(["nvidia-smi"], out / "gpu.txt")
        for netdev in selected:
            capture(probe + ["ethtool", "-i", netdev], out / f"{netdev}-driver.txt")
            # Pause queries are diagnostic; some providers do not expose them.
            result = subprocess.run(probe + ["ethtool", "-a", netdev], capture_output=True,
                                    text=True, timeout=15)
            (out / f"{netdev}-pause.txt").write_text(result.stdout + result.stderr)
        before = {net: counters(net, probe) for net in selected}
        (out / "counters-before.json").write_text(json.dumps(before, indent=2))
        for netdev in selected:
            handle = (out / f"{netdev}-mlnx_perf.log").open("w")
            handles.append(handle)
            monitor_name = name + "-" + str(len(monitors))
            monitor_names.append(monitor_name)
            monitors.append(subprocess.Popen(
                ["docker", "run", "--rm", "--name", monitor_name, "--privileged",
                 "--network=host", "--entrypoint", "/usr/bin/env", args.image,
                 "mlnx_perf", "-i", netdev, "-t", "1"],
                                              stdout=handle, stderr=subprocess.STDOUT,
                                              start_new_session=True))
        command = docker + ["--name", name, args.image,
                            "/opt/daqiri/bin/daqiri_bench_raw_gpudirect", "/results/config.yaml",
                            "--seconds", str(args.seconds), "--target-gbps", str(args.target_gbps)]
        (out / "command.json").write_text(json.dumps(command, indent=2))
        log = capture(command, out / "benchmark.log", timeout=args.seconds + 120)
        after = {net: counters(net, probe) for net in selected}
        (out / "counters-after.json").write_text(json.dumps(after, indent=2))
        deltas = {
            net: {key: value - before[net][key] for key, value in after[net].items()
                  if key in before[net]} for net in selected}
        relevant = {"tx_packets_phy", "rx_packets_phy", "tx_bytes_phy", "rx_bytes_phy",
                    "vport_loopback_packets", "vport_loopback_bytes", "rx_out_of_buffer",
                    "rx_discards_phy", "tx_discards_phy"}
        summary["counter_deltas"] = {net: {key: value for key, value in values.items()
                                          if key in relevant} for net, values in deltas.items()}
        errors = stats_errors(log) + path_errors(args.mode, tx, rx, before, after,
                                                args.strict_physical_counters)
        summary["warnings"] = []
        if args.mode == "hardware" and any(deltas[tx["netdev"]].get(k, 0)
                                            for k in ("tx_packets_phy", "rx_packets_phy")):
            summary["warnings"].append("Physical counters moved with cable attached; inspect background traffic")
        # Counter snapshots bracket startup/shutdown, so they must cover at least
        # the application's received packets, including the expected path.
        rx_counts = re.findall(r"RX complete: interface=\S+ queue=0 packets=(\d+)", log)
        path_key = "vport_loopback_packets" if args.mode == "hardware" else "rx_packets_phy"
        if rx_counts and deltas[rx["netdev"]].get(path_key, 0) < int(rx_counts[-1]):
            errors.append(f"{path_key} delta does not cover application RX packets")
        summary["application_packets"] = {
            direction: int(counts[-1]) for direction in ("TX", "RX")
            if (counts := re.findall(rf"{direction} complete: interface=\S+ queue=0 packets=(\d+)", log))}
        if any(p.poll() is not None for p in monitors):
            errors.append("mlnx_perf exited before benchmark completion; inspect monitor logs")
        summary["errors"] = errors
        if errors:
            raise RuntimeError("; ".join(errors))
        summary["status"] = "passed"
    finally:
        cleanup_errors = []
        for container in [name, *monitor_names]:
            try:
                remove_container(container)
            except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
                cleanup_errors.append(str(exc))
        for proc in monitors:
            try:
                stop_monitor(proc)
            except (OSError, subprocess.SubprocessError) as exc:
                cleanup_errors.append(str(exc))
        for handle in handles:
            handle.close()
        # The engine may raise MTUs to fit the configured frame; restore them.
        for netdev, mtu in original_mtus.items():
            if read(Path("/sys/class/net") / netdev / "mtu") != str(mtu):
                try:
                    subprocess.run(probe + ["ip", "link", "set", "dev", netdev, "mtu", str(mtu)],
                                   check=True, timeout=30)
                except (OSError, subprocess.SubprocessError) as exc:
                    cleanup_errors.append(str(exc))
        summary["cleanup_errors"] = cleanup_errors
        if cleanup_errors:
            raise RuntimeError("Cleanup failed: " + "; ".join(cleanup_errors))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--mode", choices=("hardware", "cabled"))
    parser.add_argument("--seconds", type=int)
    parser.add_argument("--image", default="daqiri:spark-ci")
    parser.add_argument("--tx", help="TX netdev or PCI BDF; otherwise discovered")
    parser.add_argument("--rx", help="RX netdev or PCI BDF; otherwise discovered")
    parser.add_argument("--cores", type=lambda s: [int(c) for c in s.split(",")])
    parser.add_argument("--target-gbps", type=float,
                        help="aggregate payload rate; default 10 for smoke testing; 0 is unpaced")
    parser.add_argument("--output", type=Path, required=True, help="new artifact directory")
    parser.add_argument("--prepare-only", action="store_true", help="discover and render without running hardware")
    parser.add_argument("--strict-physical-counters", action=argparse.BooleanOptionalAction,
                        default=None,
                        help="hardware mode: require exactly flat PHY counters on a quiet port")
    args = parser.parse_args(argv)
    defaults = dict(mode="hardware", seconds=30, target_gbps=10,
                    strict_physical_counters=False)
    try:
        profile = yaml.safe_load(args.profile.read_text())
        if not isinstance(profile, dict):
            raise ValueError("profile must contain a YAML mapping")
        if type(profile.get("schema_version")) is not int or profile["schema_version"] != 1:
            raise ValueError("profile requires schema_version: 1")
        unknown = set(profile) - {"schema_version", *defaults}
        if unknown:
            raise ValueError(f"unknown profile keys: {sorted(map(str, unknown))}")
        settings = defaults | {k: v for k, v in profile.items() if k in defaults}
        for key, value in settings.items():
            if key == "mode" and value not in ("hardware", "cabled"):
                raise ValueError("mode must be hardware or cabled")
            if key == "seconds" and (type(value) is not int or value <= 0):
                raise ValueError("seconds must be a positive integer")
            if key == "target_gbps" and (type(value) not in (int, float)
                                         or not math.isfinite(value) or value < 0):
                raise ValueError("target_gbps must be finite and nonnegative")
            if key == "strict_physical_counters" and type(value) is not bool:
                raise ValueError("strict_physical_counters must be a boolean")
            if getattr(args, key) is None:
                setattr(args, key, value)
        if args.seconds <= 0:
            raise ValueError("--seconds must be positive")
        if not math.isfinite(args.target_gbps) or args.target_gbps < 0:
            raise ValueError("--target-gbps must be finite and nonnegative")
    except (OSError, ValueError, yaml.YAMLError) as exc:
        parser.error(str(exc))
    return args


def main() -> None:
    args = parse_args()
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    try:
        run(args)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
