# DGX Spark ibverbs CI smoke tests

These tests check raw Ethernet TX/RX and flow steering with the ibverbs engine;
they do not impose a throughput threshold or validate payload contents.

Each test runs for 30 seconds and is **unpaced by default** (`target_gbps: 0`).
To limit the aggregate TX payload rate, set a positive `target_gbps` in a profile
or pass `--target-gbps 10` for a paced 10 Gb/s total run (5 Gb/s per attachment).
Both PCIe attachments run concurrently by default.

This directory provides runner scripts, test profiles, and example commands
that Blossom can invoke. It does **not** implement or register Blossom jobs.
Configure those jobs separately to handle checkout, image preparation, exclusive
hardware allocation, timeouts, and artifact archiving. The runner handles
discovery, execution, and cleanup.

Run all commands on the Spark **host, from the DAQIRI checkout root**.

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

- **Hardware loopback:** the p0 PF on each PCIe attachment transmits and receives
  internally; the two independent runs execute concurrently. Keep
  Spark's existing cable connected so its hotplug policy leaves the NIC available.
- **Cabled loopback:** two distinct physical ports (`p0`, `p1`) must have carrier,
  connected directly or through the same untagged/access switch VLAN. The switch
  must permit unicast forwarding and jumbo frames (MTU 9000 end to end). Select
  `--pairs single --tx BDF --rx BDF` for an explicitly allocated single pair.

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
ports, MTUs, and allowed CPUs. Spark exposes the same physical p0/p1 ports through
two PCIe attachments.
The runner discovers both attachments by PCI device address: hardware mode uses
p0 on each; cabled mode uses p0 → p1 within each attachment. It requires exactly
two eligible attachments and runs two benchmark containers, with a shared master
CPU and four distinct worker CPUs per container (nine allowed CPUs total).
Cabled mode rejects two PFs for the same physical port. Each generated YAML uses
one TX/RX queue, 8000-byte payloads, 64-byte headers,
256-packet bursts, and `host_pinned` buffers.

CLI settings override the checked-in profile:

| Option | Meaning |
|---|---|
| `--profile PATH` | Required YAML: `schema_version: 1`; optional `mode`, `pairs`, `seconds`, `target_gbps`, `strict_physical_counters`. |
| `--image TAG` | Image built from the tested revision; default `daqiri:spark-ci`. |
| `--output DIR` | Required new artifact directory. |
| `--mode hardware\|cabled` | Override profile topology. |
| `--pairs both\|single` | Default `both`: run both PCIe attachments concurrently. `single`: run one pair for diagnostics. |
| `--seconds N` | Override duration; default 30. |
| `--target-gbps G` | Aggregate TX payload rate in Gb/s; **default 0 is unpaced**. Positive values enable pacing. |
| `--tx NETDEV_OR_BDF`, `--rx NETDEV_OR_BDF` | Override automatic port selection. Requires `--pairs single`. Hardware mode requires the same port. |
| `--cores C0,...,C8` | Nine distinct allowed CPUs: shared master, then TX queue, RX queue, TX application, RX application for each pair. Single mode takes five CPUs. Otherwise discovered. |
| `--[no-]strict-physical-counters` | Hardware mode: require exactly flat PHY counters; default off to allow background traffic. |
| `--prepare-only` | Write discovery, generated YAML, and prepared summary without initializing hardware. |

## Results and cleanup

Exit 0 means passed (or prepared); nonzero fails the job. Archive the entire
output directory in Blossom's always/finally step. It contains `summary.json`,
`ports.json` and `pair-0/`, `pair-1/` subdirectories with per-pair `summary.json`,
resolved `config.yaml`, image/source metadata, benchmark logs,
NIC counter snapshots, and `mlnx_perf` samples, including partial timeout logs.

Pass requires successful execution, nonzero application TX/RX, zero queue CQ/drop
counters (all fields required), and receive-path counters covering application
RX: internal loopback in hardware mode, receiver PHY in cabled mode. Cabled mode
also requires increasing sender PHY counters. Hardware PHY activity is a warning
unless strict mode is enabled. NIC discard counters are diagnostics. Application
TX/RX need not match exactly at shutdown.

The top-level `summary.json` includes a compact `results` snapshot: aggregate
application TX/RX counts, pairs passed/total, queue counters, error count, and
aggregate `throughput.mean_gbps`. The runner also prints this snapshot to the job log.
Each pair summary includes throughput mean/min/max/sample count, packet counts,
maximum observed queue error/drop counters, and validation error count.

Rates come from one-second `mlnx_perf` samples, discarding the first three and last
two samples: `vport_loopback_bytes` for hardware mode and `rx_vport_unicast_bytes`
for cabled mode. The aggregate is the sum of the two per-PF stable means, an
estimate from independently sampled windows. Short runs without enough samples
report a null mean. Rates are marked `approximate`: without a NIC timestamp,
`mlnx_perf` uses the nominal interval rather than elapsed time and can report
above line rate. There is no throughput pass threshold. Cabled rates include
Ethernet headers; they are not payload-only rates. Hardware-loopback throughput
measures the internal NIC path and is distinct from wire throughput.

Physical-port counters are shared by Spark's multiplexed PF aliases: inspect
PHY rates once for wire validation, and never add the two aliases' PHY rates.
Single mode writes its config/logs directly in the output directory.

Allow five minutes per default 30-second test stage, more for longer `--seconds`
or image builds, and a SIGTERM cleanup grace period. The benchmark releases its
queues and flow rules on normal exit;
the runner removes containers, stops monitors, and restores original MTUs on
success, failure, timeout, or SIGTERM. Cleanup errors fail the job. SIGKILL or a
host reset cannot run cleanup. Other host tuning is not changed. The advisory
`/tmp/daqiri-spark-ci.lock` coordinates these jobs; Blossom must reserve hardware
against other users too.

## Optional host tuning for higher performance

Explore in priority order with the lab owner using the default unpaced tests.
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
