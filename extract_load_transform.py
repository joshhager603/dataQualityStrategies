from load_raw_data import load_raw_data
from data_profiling import employee_data_profile
from data_cleaning import clean_data
from load_clean_data import load_clean_data

if __name__ == '__main__':
    load_raw_data()
    employee_data_profile()
    clean_df = clean_data()
    employee_data_profile(clean_df)
    load_clean_data(clean_df)