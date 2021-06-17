#!/bin/bash

pyenv install 3.8.5

git clone https://github.com/glennadjrussell/airflow-demo

pyenv virtualenv 3.8.5 venv-airflow
pyenv activate venv-airflow
pip install -r ./airflow-demo/requirements.txt

