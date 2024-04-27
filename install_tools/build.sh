#!/bin/bash
set -e

sudo apt install python3.10-venv
python3 -m venv dash_env
source dash_env/bin/activate

pip install -r install_tools/requirements.txt

pre-commit install

pip install black flake8
deactivate

echo "Build process completed."
