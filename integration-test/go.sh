#!/usr/bin/env bash
set -ex

(
    cd ../client-libraries
    docker build -f Dockerfile.build-python -t modality-python-sdk-build .
)

if [ -f ~/.config/modality/license ]; then
    key=$(< ~/.config/modality/license)
    echo "MODALITY_LICENSE_KEY=${key}" > .env
fi

docker compose --profile test-collector build
docker compose --profile test-collector up  --abort-on-container-exit test-collector --remove-orphans
