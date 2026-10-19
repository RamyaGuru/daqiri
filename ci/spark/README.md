# DGX Spark ibverbs CI smoke tests

**By default, each test runs for 30 seconds, paced at 10 Gb/s aggregate payload
rate.** Set `target_gbps: 0` in a profile or pass `--target-gbps 0` for unpaced TX.
These tests check raw Ethernet TX/RX and flow steering with the ibverbs engine;
they do not impose a throughput threshold or validate payload contents.

Run all commands on the Spark **host, from the DAQIRI checkout root**. Blossom
provides checkout, image preparation, exclusive hardware allocation, timeout,
and artifact archiving; the runner handles discovery, execution, and cleanup.

## Initial provisioning (outside of Blossom jobs)

A lab administrator prepares the host once. Use the supported Spark DGX OS and
NVIDIA drivers, Docker with NVIDIA Container Toolkit, and Python 3.10+. The job
account needs Docker access for privileged, host-networked GPU containers.

If these host utilities or modules are missing:

```bash
sudo apt-get update
sudo apt-get install -y python3-venv git pciutils rdma-core ibverbs-utils nvidia-mlnx-tools
sudo modprobe mlx5_core
sudo modprobe mlx5_ib
nvidia-smi
ibv_devinfo
```

Verify Ethernet link layer in `ibv_devinfo`. If a VPI adapter needs Ethernet
configuration, use NVIDIA MFT's `mlxconfig` on the discovered BDF and reboot
before CI. These profiles use `host_pinned` memory and need no hugepage
reservation. Leave existing reservations unchanged.

- **Hardware loopback:** one NIC port transmits and receives internally. Keep
  Spark's existing cable connected so its hotplug policy leaves the NIC available.
- **Cabled loopback:** two distinct physical ports (`p0`, `p1`) must have carrier,
  connected directly or through the same untagged/access switch VLAN. The switch
  must permit unicast forwarding and jumbo frames (MTU 9000 end to end). Select
  the allocated ports explicitly if automatic discovery picks another pair.

See [Spark system configuration](../../docs/tutorials/system_configuration.md#dgx-spark-profile)
and [NVIDIA container setup](https://docs.nvidia.com/dgx/dgx-spark/nvidia-container-runtime-for-docker.html)
for provisioning details.

## Prepare the checkout and image

Use the exact tested revision. The runner imports `scripts/daqiri_config/`, so
run it from the whole checkout. Initialize `yaml-cpp` and `spdlog` submodules
unless Blossom's checkout already does so. No DAQIRI Python bindings are needed.

```bash
git submodule update --init --recursive
python3 -m venv ci-results/venv
ci-results/venv/bin/python -m pip install -r ci/spark/requirements.txt
IMAGE_TAG=daqiri:local BASE_TARGET=dpdk DAQIRI_ENGINE="dpdk ibverbs" \
  scripts/build-container.sh
docker build --build-arg DAQIRI_CI_BASE_IMAGE=daqiri:local \
  -t daqiri:spark-ci -f ci/spark/Dockerfile .
```

The tools image adds `ethtool`, `ip`, and `mlnx_perf`. Use a host driver compatible
with the base image's CUDA version (default 13.1); set `CUDA_VERSION` for the base
build if needed. Reuse the resulting image across test stages.

## Run the tests

Run sequentially on an exclusively allocated Spark. Each output directory must
be new.

```bash
DAQIRI_IMAGE=daqiri:spark-ci
ARTIFACT_DIR="ci-results/${BUILD_NUMBER:-local}"
ci-results/venv/bin/python -m ci.spark.run_loopback \
  --profile ci/spark/profiles/hardware.yaml \
  --image "$DAQIRI_IMAGE" --output "$ARTIFACT_DIR/hardware"
ci-results/venv/bin/python -m ci.spark.run_loopback \
  --profile ci/spark/profiles/cabled.yaml \
  --image "$DAQIRI_IMAGE" --output "$ARTIFACT_DIR/cabled"
```

The runner discovers mlx5 Ethernet netdevs, PCI BDFs, RDMA devices, MACs, physical
ports, MTUs, and allowed CPUs. Cabled mode rejects two PFs for the same physical
port. Generated YAML uses one TX/RX queue, 8000-byte payloads, 64-byte headers,
256-packet bursts, and `host_pinned` buffers.

CLI settings override the checked-in profile:

| Option | Meaning |
|---|---|
| `--profile PATH` | Required YAML: `schema_version: 1`; optional `mode`, `seconds`, `target_gbps`, `strict_physical_counters`. |
| `--image TAG` | Image built from the tested revision; default `daqiri:spark-ci`. |
| `--output DIR` | Required new artifact directory. |
| `--mode hardware\|cabled` | Override profile topology. |
| `--seconds N` | Override duration; default 30. |
| `--target-gbps G` | Aggregate TX payload rate; default 10, **0 disables pacing**. |
| `--tx NETDEV_OR_BDF`, `--rx NETDEV_OR_BDF` | Override automatic port selection. Hardware mode requires the same port. |
| `--cores A,B,C,D,E` | Allowed CPU IDs for master, TX queue, RX queue, TX application, RX application; otherwise discovered. |
| `--[no-]strict-physical-counters` | Hardware mode: require exactly flat PHY counters; default off to allow background traffic. |
| `--prepare-only` | Write discovery, generated YAML, and prepared summary without initializing hardware. |

## Results and cleanup

Exit 0 means passed (or prepared); nonzero fails the job. Archive the entire
output directory in Blossom's always/finally step. It contains `summary.json`,
resolved `config.yaml`, `ports.json`, image/source metadata, benchmark logs,
NIC counter snapshots, and `mlnx_perf` samples, including partial timeout logs.

Pass requires successful execution, nonzero application TX/RX, zero queue CQ/drop
counters (all fields required), and receive-path counters covering application
RX: internal loopback in hardware mode, receiver PHY in cabled mode. Cabled mode
also requires increasing sender PHY counters. Hardware PHY activity is a warning
unless strict mode is enabled. NIC discard counters are diagnostics. Application
TX/RX need not match exactly at shutdown.

For rate reporting, use stable middle-of-run one-second `mlnx_perf` samples:
`vport_loopback_bytes` for hardware mode, receiver PHY rate for cabled mode.
Count traffic once and label hardware-loopback throughput separately.

Allow five minutes per default 30-second test stage, more for longer `--seconds`
or image builds, and a SIGTERM cleanup grace period. The benchmark releases its
queues and flow rules on normal exit;
the runner removes containers, stops monitors, and restores original MTUs on
success, failure, timeout, or SIGTERM. Cleanup errors fail the job. SIGKILL or a
host reset cannot run cleanup. Other host tuning is not changed. The advisory
`/tmp/daqiri-spark-ci.lock` coordinates these jobs; Blossom must reserve hardware
against other users too.

## Optional host tuning for higher performance

Explore in priority order with the lab owner using higher-rate or unpaced tests.
These changes are not automated; coordinate shared-host impact and restoration.
Procedures are in the
[Spark configuration guide](../../docs/tutorials/system_configuration.md#dgx-spark-profile).

| Priority | Tuning | Scope / impact |
|---|---|---|
| 1 | Pin queue/application workers to allocated high-frequency CPUs using `--cores`. | Job-scoped; reserve cores against competing work. |
| 2 | Verify CPU governor is `performance`. | Changes affect shared CPU policies, power, and thermals; restore prior settings. |
| 3 | Investigate pause counters; consider disabling pause on dedicated ports/peers if throttled. | Port/switch-wide; coordinate other raw/RDMA traffic and restore policy. |
| 4 | Investigate GPU SM clock locking if clocks affect results. | Device-wide power/thermal impact; use an exclusive window and reset afterward. |
| 5 | Consider boot CPU isolation and IRQ/RCU placement. | Persistent scheduling changes, reduced shared CPU capacity, and reboot required. |
| 6 | Inspect PCIe speed/MPS and evaluate MRRS tuning. | Device-wide; record and restore original settings. |
| 7 | Compare `kind: huge` buffers if needed. | Reservations consume shared RAM and survive container teardown. |
