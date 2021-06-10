# airflow-demo

# Installation
* Install pyenv https://github.com/pyenv/pyenv#homebrew-on-macos
* Install pyenv-virtualenv
* Install python `pyenv install 3.8.5
* Create virtualenv `pyenv virtualenv 3.8.5 venv-airflow`
* Activate `pyenv activate venv-airflow`
* Install requirements `pip install -f requirements.txt`
* Initialise Airflow `airflow initdb`
* Start scheduler `airflow scheduler`
* In a seperate window `airflow webserver -p 8090
* Open the admin panel `open http://localhost:8090`


