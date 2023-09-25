<!--
SPDX-FileCopyrightText: Magenta ApS
SPDX-License-Identifier: MPL-2.0
-->

# SDLøn

This folder contains the SD-changed-at (including the SD-importer) and the
SDTool integrations.

# Building

The [.gitlab-ci.yml](.gitlab-ci.yml) takes care of building a Docker image
for the SD-changed-at integration and a Docker image for the SDTool
integration. The containers created from these two images are run as separate
Docker applications on the customer servers and in the clusters.
