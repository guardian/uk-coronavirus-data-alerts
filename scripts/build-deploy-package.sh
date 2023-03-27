#!/bin/bash
set -e

# The container doesn't come with pipenv so we install it up front.
# If we end up with more than one Python project we could create a base container for it.
pip install pipenv
pipenv sync

# https://stackoverflow.com/questions/51679863/package-python-pipenv-project-for-aws-lambda
SITE_PACKAGES=$(pipenv --venv)/lib/python*/site-packages
BUILD_DIR=$(pwd)/dist/lambda

mkdir -p $BUILD_DIR

# TODO MRB: get pipenv sync to install the dependencies directly into the right location
echo -n "Copying dependencies to lambda bundle (this might take a while)... "
cp -r $SITE_PACKAGES/* $BUILD_DIR
echo "Done"

cp -r src/*.py $BUILD_DIR

