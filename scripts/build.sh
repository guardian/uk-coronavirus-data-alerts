#!/bin/bash
set -e

mkdir -p dist

# Run the build inside Docker as Numpy (via Pandas) will install
# binary libraries per OS architecture per Python version
docker run \
    -w /build \
    --mount type=bind,src=$(pwd),dst=/build \
    python:3.8-slim \
    scripts/build-deploy-package.sh

npm install
npm run synth

BUILD_DIR=$(pwd)/dist/lambda
CA_VERIFIED_DEST=${BUILD_DIR}/uk-coronavirus-data-alerts-verified.zip
CA_UNVERIFIED_DEST=${BUILD_DIR}/uk-coronavirus-data-alerts-unverified.zip

zip -r $CA_VERIFIED_DEST $BUILD_DIR

# We use the same zip for both verified/unverified
cp $CA_VERIFIED_DEST $CA_UNVERIFIED_DEST
