#!/usr/bin/env bash
set -ex

(
    cd ..
    docker build -f Dockerfile.build -t modality-dlt-build .
)

if [ -f ~/.config/modality/license ]; then
    key=$(< ~/.config/modality/license)
    echo "MODALITY_LICENSE_KEY=${key}" > .env
fi

docker compose --profile test-collector build
docker compose --profile test-import build

docker compose --profile test-collector up  --abort-on-container-exit test-collector
docker compose --profile test-import up --abort-on-container-exit test-import
