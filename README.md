storageprovider-client
======================

[![image](https://travis-ci.org/OnroerendErfgoed/storageprovider-client.svg)](https://travis-ci.org/OnroerendErfgoed/storageprovider-client)

Storageprovider-client is a library that provides methods to communicate
with a Storageprovider instance.

## How to work with pip-compile / pip-sync
full docs: https://pip-tools.readthedocs.io/en/latest/

First install the package:
```sh
pip install pip-tools
```

#### local development
Create a virutal environment and sync with the requirements-dev.txt file.

```sh
pip-sync requirements-dev.txt
```
This will install all the packages needed for development, including testing libraries and waitress.

Fast pip-compile
```sh
PIP_COMPILE_ARGS="-v --no-header --strip-extras --no-emit-find-links pyproject.toml"
uv pip compile $PIP_COMPILE_ARGS -o requirements.txt
uv pip compile $PIP_COMPILE_ARGS -o requirements-dev.txt --all-extras
```