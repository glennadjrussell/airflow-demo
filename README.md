# airflow-demo

# Installation on Windows
* Install Windows Linux Subsystem
* Install & launch Ubuntu 18.04 LTS

## From the Ubuntu terminal screen
### Install dependencies
```
$ sudo apt-get update
$ sudo apt install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python-openssl git
```

### Install pyenv and virtualenv
```
$ curl https://pyenv.run | bash
$ echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
$ echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
$ echo 'eval "$(pyenv init --path)"' >> ~/.bashrc
$ echo 'eval "$(pyenv init -)"' >> ~/.bashrc
$ echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bashrc
```

Restart the shell

### Create a virtual environment and install python libraries
```
$ pyenv install 3.8.5
$ pyenv virtualenv 3.8.5 venv-airflow
$ pyenv activate venv-airflow
$ pip install -r airflow-demo/requirements.txt
```

### Initialise Airflow
```
$ airflow initdb
```

Launch a second terminal window

In the first terminal
```
$ pyenv activate venv-airflow
$ airflow scheduler
```

In the second terminal
```
$ pyenv activate venv-airflow
$ airflow webserver -p 9090
```

Open a web browser and point to `http://localhost:9090`

### Configure gcloud access
This will allow your Airflow instance to access GCloud APIs based on your user credentials

```
$ echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
$ sudo apt-get install apt-transport-https ca-certificates gnupg
$ curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
$ sudo apt-get update && sudo apt-get install google-cloud-sdk
$ gcloud auth application-default login
```

# Installation
* Install pyenv https://github.com/pyenv/pyenv#homebrew-on-macos
* Install pyenv-virtualenv
* Install python `pyenv install 3.8.5`
* Create virtualenv `pyenv virtualenv 3.8.5 venv-airflow`
* Activate `pyenv activate venv-airflow`
* Install requirements `pip install -f requirements.txt`
* Initialise Airflow `airflow initdb`
* Start scheduler `airflow scheduler`
* In a seperate window `airflow webserver -p 8090
* Open the admin panel `open http://localhost:8090`


