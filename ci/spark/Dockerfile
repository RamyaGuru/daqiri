# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-License-Identifier: Apache-2.0
# Monitoring tools for ci.spark.run_loopback.
ARG DAQIRI_CI_BASE_IMAGE=daqiri:local
FROM ${DAQIRI_CI_BASE_IMAGE}
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    ethtool iproute2 mlnx-tools \
    && rm -rf /var/lib/apt/lists/*
