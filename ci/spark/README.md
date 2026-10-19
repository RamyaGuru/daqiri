# DGX Spark ibverbs CI smoke tests

Run these commands on the Spark **host, from the DAQIRI checkout root**. Blossom
owns checkout, image preparation, exclusive Spark allocation, outer timeout, and
artifact archiving. The runner owns discovery, YAML generation, the timed test,
counter checks, and cleanup. Both profiles run for 30 seconds at a paced 10 Gb/s;
they are functional smoke tests without a performance threshold.

## Provision once, outside Blossom jobs

A lab administrator completes this section when preparing or repairing the host.
These package installs, driver/module setup, firmware changes, and reboots are
**not steps in the Blossom build or benchmark jobs**. CI assumes a provisioned
Spark and only checks/accesses its hardware.

Use the supported Spark DGX OS/NVIDIA driver stack with Docker and NVIDIA
Container Toolkit. The job account needs Docker access; it runs privileged,
host-networked GPU containers. Host Python 3.10+ and `python3-venv` are required.
Keep Spark's matching mlx5 drivers and NVIDIA package repositories. See the
[Spark system configuration](../../docs/tutorials/system_configuration.md#dgx-spark-profile)
and [NVIDIA container runtime guide](https://docs.nvidia.com/dgx/dgx-spark/nvidia-container-runtime-for-docker.html).

If host utilities/modules are missing, a provisioning administrator can run:

```bash
sudo apt-get update
sudo apt-get install -y python3-venv git pciutils rdma-core ibverbs-utils nvidia-mlnx-tools
sudo modprobe mlx5_core
sudo modprobe mlx5_ib
nvidia-smi
ibv_devinfo
```

Verify Ethernet link layer in `ibv_devinfo` or
`/sys/class/infiniband/<device>/ports/*/link_layer`. If a VPI adapter needs a
change, query its discovered BDF with NVIDIA MFT's `mlxconfig`, set
`LINK_TYPE_P1=ETH LINK_TYPE_P2=ETH`, and reboot before CI. Ethernet-only firmware
may omit those options. Standard UDP steering needs no flex-parser settings.

- **Hardware profile:** TX/RX use one enumerated port. Keep the existing cable
  connected: Spark can remove its CX-7 PCI devices when no cable is detected.
  Test traffic returns internally; this job does not override hotplug policy.
- **Cabled profile:** use two distinct physical ports (`p0`, `p1`), connected
  directly or through the same untagged/access switch VLAN. Both links must have
  carrier. The switch must permit MAC learning/unicast forwarding and jumbo
  frames (configure MTU 9000 end to end). Set explicit port overrides for the
  lab's allocated switch path; carrier alone does not prove VLAN connectivity.

These profiles use the ibverbs engine and `host_pinned` buffers, allocated with
`cudaHostAlloc`. Pinned memory does not require a reserved hugetlb pool. The
ibverbs metadata pools/rings also use ordinary aligned allocations. Therefore
these profiles do not request hugetlb allocations and are expected to run without
a hugepage reservation or `/dev/hugepages` mount under default provider settings,
even though the image also contains the DPDK engine. The mlx5 provider can also
use hugepages for internal buffers; provider overrides that force hugepage
allocation have different requirements. A DPDK run or a config using `kind: huge`
also has different requirements. A 10-second hardware-loopback test passed with
all HugeTLB usage and reservation limits set to zero in a private child cgroup
inside the benchmark container. An explicit 1 GiB hugepage allocation failed
with `ENOMEM`, while the benchmark sent/received 1,551,104 packets and HugeTLB
usage stayed zero. The host's four reserved 1 GiB pages remained free and unchanged.
This verifies operation without access to reserved hugetlb pages for that run;
it does not disable transparent hugepages. Evidence and the test wrapper are in
ignored `ci-results/no-hugepages-validation/`. Leave existing host reservations alone.
Hugepage reservation is **not a prerequisite for these CI profiles** and the job
does not create, resize, or remove the host's pool.
Wendell reports that the lab Spark already has hugepages enabled; leave that
provisioned pool available for normal CI runs. Its size/count need not match the
local test host for these `host_pinned` profiles.
No peermem, VFIO binding, host IP/routing changes, or namespaces are needed.
CPU/governor/pause tuning is optional at this smoke-test rate. The engine may
raise host MTUs; cleanup restores them.

## Checkout, dependencies, and image

Use the exact tested revision and its submodules. The **whole checkout** is the
deployment unit: the runner imports `scripts/daqiri_config/` from that revision.
Do not copy the runner alone into the Blossom repository. No DAQIRI Python
bindings or Jupyter packages are required.

`git submodule update --init --recursive` fetches the `yaml-cpp` and `spdlog`
versions pinned by the checked-out DAQIRI revision for the container build.
Blossom can omit this command if its checkout step already initializes submodules
at those revisions; it is not needed again in each benchmark stage.

```bash
git submodule update --init --recursive
python3 -m venv ci-results/venv
ci-results/venv/bin/python -m pip install -r ci/spark/requirements.txt

IMAGE_TAG=daqiri:local BASE_TARGET=dpdk DAQIRI_ENGINE="dpdk ibverbs" \
  scripts/build-container.sh
docker build --build-arg DAQIRI_CI_BASE_IMAGE=daqiri:local \
  -t daqiri:spark-ci -f ci/spark/Dockerfile .
```

The tools layer supplies `ethtool`, `ip`, and `mlnx_perf`. Use a host driver
compatible with the build's CUDA version (currently 13.1 by default); set
`CUDA_VERSION` for the base build if needed. Blossom can build once and pass the
resulting image tag to each test stage.

## Host tuning to discuss with Wendell (optional, in priority order)

This is an investigation list, not additional provisioning requirements or
changes performed by the runner. Agree on device/core allocation and rollback
with the lab owner before trials. The current 10 Gb/s paced smoke test is not a
maximum-throughput benchmark: compare baseline and one change at a time in an
agreed performance window at a higher target rate (or unpaced), retaining the
same packet/configuration settings and stable NIC-counter samples.

| Priority | Explore | Scope and impact on other jobs |
|---|---|---|
| 1 | **CPU placement and scheduling:** allocate four high-frequency cores for queue/application workers plus an allowed master core; use `--cores` within Blossom's CPU allocation. | Affinity is job-scoped and already supported. Reserve the allocated cores against competing jobs; pinning alone does not isolate them. No reboot or boot-parameter change needed. |
| 2 | **CPU governor:** first verify `performance`, the documented Spark default; compare only if the lab uses another policy. | Reading is harmless. Changing it can affect other jobs sharing a CPU frequency policy, power, and thermals; save/restore the prior governor or make it an agreed host baseline. |
| 3 | **Ethernet pause:** inspect saved pause state/counters, then consider disabling link-level pause on dedicated test ports and their switch peers if it is throttling traffic. | Port/switch-wide, not container-scoped. Other raw/RDMA traffic can depend on flow control. Coordinate with the network owner and restore the full prior policy; the runner currently only observes it. |
| 4 | **GPU SM clocks / idle behavior:** investigate only if clock variation correlates with rate variation. Spark guidance covers SM clock locking; a separate VRAM clock lock does not apply. | Device-wide power/thermal and scheduling effects on every GPU job. Trial in an exclusive window with an agreed reset/baseline; the runner does not manage clocks. |
| 5 | **Boot CPU isolation and IRQ/RCU placement:** consider `isolcpus`, `nohz_full`, RCU offload, and IRQ affinity if job-level affinity still leaves measurable interference. | Persistent host scheduling changes and reboot required. Removes/restricts cores available to other CI jobs and requires a separate housekeeping-core plan and boot rollback. |
| 6 | **NIC PCIe settings / MRRS:** inspect negotiated speed, MPS, and current MRRS first; consider the Spark guide's 4096-byte MRRS setting only with measured benefit. | PF/device-wide setting, not container-scoped. The guide's procedure may require disabling Secure Boot and a persistent service. Needs lab/security approval and saved-register rollback; do not reset the NIC during shared jobs. |
| 7 | **Hugepage-backed memory:** revisit only for future DPDK tests or an explicit `kind: huge` comparison. | Deferred for the current `host_pinned` profiles. Reservations remove RAM from general use and survive container teardown; boot configuration can require a reboot. Never resize a shared pool as routine job cleanup. |

Procedures and Spark-specific caveats are in the
[Spark system-optimization section](../../docs/tutorials/system_configuration.md#dgx-spark-profile).
The runner currently restores MTU only; any additional tuning needs its own
agreed restoration or administrator-managed baseline before entering CI.

## Blossom test stages

Run sequentially on a reserved Spark. Each output directory must be new:

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

Checked-in profiles contain test policy; CLI overrides take precedence. Options:
`--seconds 30`, `--target-gbps 10` (`0` disables pacing), `--tx NETDEV_OR_BDF`,
`--rx NETDEV_OR_BDF`, `--cores 5,6,7,8,9`, and
`--[no-]strict-physical-counters`. Leave ports/cores unset for discovery. Set TX
and RX to the lab's allocated netdevs/BDFs for a switch run; the cabled runner
rejects two PFs exposing the same physical port. `--prepare-only` writes
`ports.json`, concrete `config.yaml`, and a prepared summary without running hardware.
Discovery is implemented in the runner's `discover()` and `select_ports()`:
sysfs provides PCI BDFs, mlx5 driver/RDMA associations, Ethernet link layer,
physical-port names, MACs, MTUs, and carrier; CPU selection uses the process's
allowed affinity and advertised frequencies. It runs for every invocation.

## Results and recovery

Exit `0` means passed (or prepared with `--prepare-only`); nonzero fails the job.
Archive the entire output directory in Blossom's **always/finally** step, even on
failure. `summary.json` records schema version, effective settings, discovered
devices, source revision, packet counts, counter deltas, warnings, and cleanup
errors. `image.json` records the image ID; partial logs survive timeouts.

Pass requires successful execution, nonzero application TX/RX, present and zero
queue CQ/drop counters, and counters covering the expected receive path. Hardware
mode requires internal-loopback packets; cabled mode requires physical TX/RX.
Hardware PHY activity is a warning with a cable attached; use strict physical
counters only on a quiet port. NIC discard/out-of-buffer deltas are retained as
diagnostics, not pass criteria. This test does not check payload integrity or
require exact application TX/RX equality at shutdown.

Inspect stable middle-of-run one-second `mlnx_perf` samples, excluding startup
and shutdown. Hardware mode uses `vport_loopback_bytes`; cabled mode uses receiver
PHY rate. Count traffic once and label hardware-loopback throughput separately.

Allow five minutes per test stage and a termination grace period for SIGTERM
cleanup; allow longer for image builds. At normal completion the benchmark stops
and joins its worker threads, prints statistics, and calls `daqiri::shutdown()`
to destroy its steering rules, queues, memory registrations, and temporary
hardware-loopback activation QP. The runner then removes its containers, stops
monitors, and restores each selected netdev's MTU captured before initialization.
Timeouts, errors, and SIGTERM also enter the runner's cleanup path; forced
container removal does not guarantee the benchmark's graceful shutdown sequence.
Cleanup failures fail the job and are recorded in `summary.json`.

MTU is the only persistent host setting this runner changes and restores. CPU
affinity and memlock limits are container-scoped. It does not change firmware,
host IPs/routes, namespaces, hugepage reservations, CPU governors, pause policy,
or switch settings. Provisioning changes remain in place; logs, virtualenvs,
and built images are retained. SIGKILL or a host reset cannot run cleanup.
`/tmp/daqiri-spark-ci.lock` serializes these jobs under one account; Blossom must
also reserve the machine against other workloads.

Both profile-based 30-second variants passed their final check on `spark-f2d7`
using its 200G direct loopback cable and the CI tools layer over an existing
DAQIRI image. Each reported TX=RX=4,653,056 packets with zero reported queue
errors/drops; stable NIC-counter samples averaged about 10.15 Gb/s at the paced
10 Gb/s target. MTUs and the recorded persistent host state were unchanged after
cleanup, including four free 1 GiB hugepages. Local results and before/after
snapshots are in ignored `ci-results/final-validation-20261019/`.

The local host retains pre-existing CPU isolation and MRRS tuning; the runner
used allowed housekeeping cores 0-4. The Slack thread confirms hugepages and
switch cabling on the lab Spark but does not establish its other tuning values.
No speculative rollback was applied. Fresh-host provisioning and the lab switch
path still need validation; this test does not establish exact host parity or
verify the lab switch VLAN/MTU settings.
