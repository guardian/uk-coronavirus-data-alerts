#!/bin/bash
set -e

export NVM_DIR="$HOME/.nvm"
[[ -s "$NVM_DIR/nvm.sh" ]] && . "$NVM_DIR/nvm.sh"  # This loads nvm

mkdir -p dist

# Run the build inside Docker as Numpy (via Pandas) will install
# binary libraries per OS architecture per Python version
docker run \
    -w /build \
    --mount type=bind,src=$(pwd),dst=/build \
    python:3.8-slim \
    scripts/build-deploy-package.sh

nvm use
npm install
npm run synth

./node_modules/.bin/node-riffraff-artifact