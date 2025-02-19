# CSDS 397 Individual Assignment 2: Data Quality Strategies
**Josh Hager (jrh236)**

## Description
This repository holds code to extract, load, and clean the employee_data_source.csv file. It utilizes a PostgreSQL database to store the data, and performs loads/transforms using Python. All required packages are listed in `requirements.txt`.

## Pre-requisites
To run the code in this repository, you must:

1. Have an installation of Python on your machine (https://www.python.org/downloads/).
2. Have an installation of PostgreSQL on your machine. If you don't, you can run `brew install postgresql` on MacOS to install using Homebrew. Otherwise, see https://www.postgresql.org/download/.
3. Have PostgreSQL started on your machine. On MacOS, you can run `brew services start postgresql`.

## Instructions
1. Clone this git repo to your machine using: 
    ```
    git clone https://github.com/joshhager603/dataQualityStrategies.git
    ```
2. In a terminal, `cd` into the repo you just cloned.
3. Run `python3 -m venv .venv` to create a virtual environment.
4. Activate the virtual environment using `source ./.venv/bin/activate` on Linux/Mac, or `./.venv/Scripts/activate` on Windows.
5. Install the required packages using `pip install -r requirements.txt`
6. In the file `constants.py`, change the `POSTGRES_USER` and `POSTGRES_PASS` variables to the username and password you use for PostgreSQL on this machine. You may also need to change `POSTGRES_HOST` and `POSTGRES_PORT` if you have changed these from the default.
7. Run the following command to extract the data from the `employee_data_source.csv` file, profile it, clean it, and then save the cleaned data back to PostgreSQL as well as to a `.csv` file.

    ```
    python3 extract_load_transform.py
    ```
