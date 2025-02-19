import pandas as pd
from constants import *
import common_functions
import re
from dateutil import parser

def valid_date(string) -> bool:
    if type(string) == pd.Timestamp:
        return True

    try:
        parser.parse(string)
        return True
    except ValueError:
        return False

def profile_column(df: pd.DataFrame,
                   column_name: str, 
                   data_format_regex: str=None, 
                   check_for_groups: bool=False,
                   allow_duplicates=True,
                   allow_blanks=True
                   ):
    
    print(f"\n----- Profiling column {column_name} -----")

    column = df[column_name]

    if data_format_regex is not None:
        bad_format = [entry for entry in column.to_list() if not re.match(data_format_regex, str(entry))]

        if len(bad_format) != 0:
            print(f'* Found {len(bad_format)} entries that do not conform to regex {data_format_regex}:')
            print(bad_format)
        else: 
            print(f'* No format issues, all entries conform to regex {data_format_regex}')

        print()

    if check_for_groups:
        groups = column.value_counts()

        print(f'* Found {len(groups)} groups in column {column_name}:')
        print(groups)
        print()

    if not allow_duplicates:
        num_duplicates = column.duplicated().sum()

        print(f'* Found {num_duplicates} duplicate entries for column {column_name}')
        print()

    if not allow_blanks:
        num_empty = column.astype(str).str.strip().eq('').sum()
        num_missing = column.isna().sum()

        print(f'* Found {num_empty + num_missing} empty or missing entries for column {column_name}')
        print()

def employee_data_profile(df=None):

    if df is None:
        # pull the raw data from postgres
        engine = common_functions.create_sqlalchemy_engine()
        df = pd.read_sql(f'SELECT * FROM {RAW_DATA_TABLE_NAME}', engine)

    # checks for Employee Id:
    # 1. No blanks
    # 2. No duplicates
    # 3. All positive integers
    profile_column(df, 'Employee Id', data_format_regex="^[0-9]+$", allow_duplicates=False, allow_blanks=False)
    print()

    # checks for Name:
    # 1. No blanks
    profile_column(df, 'Name', allow_blanks=False)
    print()

    # checks for Age:
    # 1. No blanks
    # 2. All 1, 2, or 3 digit positive integers
    profile_column(df, 'Age', data_format_regex="^[0-9]{1,3}$", allow_blanks=False)
    print()

    # checks for Department
    # 1. No blanks
    # 2. Check for groups
    profile_column(df, "Department", check_for_groups=True, allow_blanks=False)
    print()

    # checks for Date of Joining
    # 1. No blanks
    # 2. All entries conform to YYYY-MM-DD date format
    # 3. All entries are valid dates
    profile_column(df, "Date of Joining", allow_blanks=False, data_format_regex="^[0-9]{4}-[0-9]{2}-[0-9]{2}")

    invalid_dates = [entry for entry in df["Date of Joining"].to_list() if not valid_date(entry)]

    if len(invalid_dates) == 0:
        print("* No invalid dates found for column Date of Joining\n")
    else:
        print(f'* Found {len(invalid_dates)} invalid dates in column Date of Joining:')
        print(invalid_dates)

    print()

    # checks for Years of Experience
    # 1. No blanks
    # 2. All entries are 1, 2, or 3 digit positive integers
    profile_column(df, 'Years of Experience', data_format_regex="^[0-9]{1,3}.0$|^[0-9]{1,3}$", allow_blanks=False)
    print()

    # checks for Country:
    # 1. No blanks
    # 2. Check for groups
    profile_column(df, "Country", check_for_groups=True, allow_blanks=False)
    print()

    # checks for Salary:
    # 1. No blanks
    # 2. All positive integers
    profile_column(df, 'Salary', data_format_regex="^[0-9]+$", allow_blanks=False)

    # checks for Performance Rating:
    # 1. No blanks
    # 2. Check for groups
    profile_column(df, "Performance Rating", check_for_groups=True, allow_blanks=False)
    print()

if __name__ == '__main__':
    employee_data_profile()
