# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-License-Identifier: Apache-2.0
"""Hardware-free checks for Spark CI topology and failure gates."""

import json
from pathlib import Path
import signal
import subprocess
import sys
from types import SimpleNamespace

import pytest

from ci.spark import run_loopback as runner
from ci.spark.run_loopback import make_config, path_errors, select_ports, stats_errors


def port(name, physical, mac, carrier=True):
    return dict(netdev=name, bdf=name, rdma="roce0", physical_port=physical,
                mac=mac, mtu=9000, ethernet=True, carrier=carrier)


@pytest.fixture
def ports():
    return [port("a", "p0", "02:00:00:00:00:01"),
            port("b", "p0", "02:00:00:00:00:02"),
            port("c", "p1", "02:00:00:00:00:03")]


def test_cabled_avoids_alias_pf_and_requires_carrier(ports):
    assert select_ports(ports, "cabled", None, None) == (ports[0], ports[2])
    with pytest.raises(ValueError):
        select_ports(ports, "cabled", "a", "b")
    ports[2]["carrier"] = False
    with pytest.raises(ValueError):
        select_ports(ports, "cabled", None, None)


def test_hardware_accepts_enumerated_port_without_carrier(ports):
    ports[0]["carrier"] = False
    assert select_ports(ports, "hardware", "a", None) == (ports[0], ports[0])
    with pytest.raises(ValueError):
        select_ports([], "hardware", None, None)


@pytest.mark.parametrize("mode", ["hardware", "cabled"])
def test_generated_topology_and_spark_memory(mode, ports):
    tx, rx = select_ports(ports, mode, None, None)
    document = make_config(mode, tx, rx, [5, 6, 7, 8, 9])
    cfg = document["daqiri"]["cfg"]
    assert cfg["engine"] == "ibverbs"
    assert all(m["kind"] == "host_pinned" for m in cfg["memory_regions"])
    assert len(cfg["interfaces"]) == (1 if mode == "hardware" else 2)
    assert cfg["loopback"] == ("hw" if mode == "hardware" else "")
    assert document["bench_rx"][0]["interface_name"] == (
        "tx_port" if mode == "hardware" else "rx_port")


def test_packet_error_and_path_gates(ports):
    good = "TX complete: interface=tx_port queue=0 packets=123\n" \
           "RX complete: interface=rx_port queue=0 packets=123\n" \
           "cqe_errors=0 app_ring_full_drops=0 handoff_full_drops=0\n"
    assert stats_errors(good) == []
    assert stats_errors(good.replace("RX complete", "missing"))
    assert stats_errors(good.replace("packets=123", "packets=0"))
    assert stats_errors(good + "cqe_errors=1 app_ring_full_drops=2")
    before = {p["netdev"]: {"tx_packets_phy": 100, "rx_packets_phy": 100,
                           "vport_loopback_packets": 0} for p in ports}
    after = {p["netdev"]: {"tx_packets_phy": 200, "rx_packets_phy": 200,
                          "vport_loopback_packets": 100} for p in ports}
    assert not path_errors("cabled", ports[0], ports[2], before, after)
    assert path_errors("hardware", ports[0], ports[0], before, after, True)
    assert not path_errors("hardware", ports[0], ports[0], before, after)
    assert path_errors("hardware", ports[0], ports[0], before, before)
    assert path_errors("cabled", ports[0], ports[2], before, before)
    assert path_errors("hardware", ports[0], ports[0], {"a": {}}, {"a": {}})


def test_missing_queue_fields_fail():
    assert stats_errors("TX complete: interface=x queue=0 packets=1\n"
                        "RX complete: interface=x queue=0 packets=1\n") == [
        "Missing required queue counter cqe_errors",
        "Missing required queue counter app_ring_full_drops",
        "Missing required queue counter handoff_full_drops",
    ]


@pytest.mark.parametrize("mode", ["hardware", "cabled"])
def test_checked_in_profiles_and_cli_precedence(mode, tmp_path):
    root = Path(__file__).resolve().parents[2]
    args = runner.parse_args([
        "--profile", str(root / f"ci/spark/profiles/{mode}.yaml"),
        "--output", str(tmp_path / "result"),
    ])
    assert (args.mode, args.seconds, args.target_gbps) == (mode, 30, 0)
    args = runner.parse_args([
        "--profile", str(root / f"ci/spark/profiles/{mode}.yaml"),
        "--output", str(tmp_path / "result"), "--seconds", "2",
        "--target-gbps", "1", "--strict-physical-counters",
    ])
    assert (args.seconds, args.target_gbps, args.strict_physical_counters) == (2, 1, True)


@pytest.mark.parametrize("setting", ["target_gbps: .nan", "seconds: 0", "typo: 1",
                                     "strict_physical_counters: yesplease"])
def test_bad_profiles_fail_before_hardware(tmp_path, setting):
    profile = tmp_path / "profile.yaml"
    profile.write_text("schema_version: 1\n" + setting + "\n")
    with pytest.raises(SystemExit) as exc:
        runner.parse_args(["--profile", str(profile), "--output", str(tmp_path / "out")])
    assert exc.value.code == 2
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("stderr", ["Cannot connect to the Docker daemon", "removal failed"])
def test_docker_removal_failure_is_reported(monkeypatch, stderr):
    monkeypatch.setattr(runner.subprocess, "run", lambda *a, **k:
                        subprocess.CompletedProcess(a[0], 1, "", stderr))
    with pytest.raises(RuntimeError, match="Cannot remove container"):
        runner.remove_container("test-container")


def test_already_removed_container_is_ok(monkeypatch):
    monkeypatch.setattr(runner.subprocess, "run", lambda *a, **k:
                        subprocess.CompletedProcess(a[0], 1, "", "No such container: test"))
    runner.remove_container("test")


def test_timeout_retains_partial_log(tmp_path):
    log = tmp_path / "partial.log"
    with pytest.raises(subprocess.TimeoutExpired):
        runner.capture([sys.executable, "-c",
                        "import time; print('started', flush=True); time.sleep(20)"],
                       log, timeout=1)
    assert "started" in log.read_text()


@pytest.mark.parametrize("failure", [subprocess.TimeoutExpired("benchmark", 150), SystemExit(143)])
@pytest.mark.parametrize("cleanup_failure", [False, True])
def test_interrupted_benchmark_cleans_up_and_restores_mtu(monkeypatch, tmp_path, ports, failure,
                                                        cleanup_failure):
    removed, stopped, commands = [], [], []
    mtus = iter(["1500", "9000"])
    monkeypatch.setattr(runner, "read", lambda *a: next(mtus))
    monkeypatch.setattr(runner, "counters", lambda *a: {})
    def remove(name):
        removed.append(name)
        if cleanup_failure and len(removed) == 1:
            raise RuntimeError("cannot remove benchmark container")

    monkeypatch.setattr(runner, "remove_container", remove)
    monkeypatch.setattr(runner, "stop_monitor", stopped.append)
    monkeypatch.setattr(runner.subprocess, "Popen", lambda *a, **k: SimpleNamespace(pid=123))

    def fake_run(command, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    def fake_capture(command, path, **kwargs):
        if "/opt/daqiri/bin/daqiri_bench_raw_gpudirect" in command:
            raise failure
        return "ibverbs" if "--list-engines" in command else ""

    monkeypatch.setattr(runner.subprocess, "run", fake_run)
    monkeypatch.setattr(runner, "capture", fake_capture)
    args = SimpleNamespace(image="test", seconds=30, target_gbps=10)
    summary = {}
    with pytest.raises(RuntimeError if cleanup_failure else type(failure)):
        runner.execute(args, tmp_path, ports[0], ports[0], [0, 1, 2, 3, 4], summary)
    assert len(removed) == 2  # benchmark and one monitor
    assert len(stopped) == 1
    assert any(command[-6:] == ["link", "set", "dev", "a", "mtu", "1500"]
               for command in commands)
    assert summary["cleanup_errors"] == (["cannot remove benchmark container"]
                                         if cleanup_failure else [])


def test_sigterm_exits_with_cleanup_compatible_status(monkeypatch):
    handlers = {}
    monkeypatch.setattr(runner, "parse_args", lambda: SimpleNamespace())
    monkeypatch.setattr(runner.signal, "signal", lambda signum, handler: handlers.update({signum: handler}))
    monkeypatch.setattr(runner, "run", lambda args: handlers[signal.SIGTERM](signal.SIGTERM, None))
    with pytest.raises(SystemExit) as exc:
        runner.main()
    assert exc.value.code == 143


def test_cancelled_run_retains_failure_summary(monkeypatch, tmp_path, ports):
    args = SimpleNamespace(output=tmp_path / "out", mode="hardware", seconds=30,
                           profile=Path("ci/spark/profiles/hardware.yaml"),
                           strict_physical_counters=False, tx=None, rx=None,
                           cores=[0, 1, 2, 3, 4], image="test", target_gbps=10, prepare_only=False)
    monkeypatch.setattr(runner, "discover", lambda: ports)
    monkeypatch.setattr(runner.os, "sched_getaffinity", lambda _: set(args.cores))
    monkeypatch.setattr(runner.subprocess, "run", lambda *a, **k:
                        subprocess.CompletedProcess(a[0], 0, "revision\n", ""))

    def cancel(*args):
        raise SystemExit(143)

    monkeypatch.setattr(runner, "execute", cancel)
    with pytest.raises(SystemExit):
        runner.run(args)
    summary = json.loads((args.output / "summary.json").read_text())
    assert summary["status"] == "failed"
    assert summary["failure"] == "143"
