# UK Coronavirus Data Alerts

## Running locally

This project uses Python 3. See the [Hitchhiker's Guide to Python](https://docs.python-guide.org/starting/installation/) for options on how to install it.

It also uses [pipenv](https://docs.python-guide.org/dev/virtualenvs/#installing-pipenv) to manage dependencies, virtualenvs etc.

You will need `investigations` credentials from Janus.

First install the dependencies:

```bash
pipenv install
```

Then run the script:

```bash
pipenv run python src/main.py
```

## Deployment

`main.py` runs on a schedule as a lambda function in AWS. It is defined using CDK in [src/aws-infra.ts](src/aws-infra.ts) and automatically deployed using Riff-Raff.

To generate the CloudFormation template locally, you'll need at least [node.js 14](https://nodejs.org/en/) installed. [nvm](https://github.com/nvm-sh/nvm) is a useful tool for this.

```bash
# If you installed node.js using nvm
nvm use

# In all cases
npm i
npm run synth

# The cloudformation template is in `dist/cloudformation.yaml`
```