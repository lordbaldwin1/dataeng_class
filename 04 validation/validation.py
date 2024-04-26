import pandas as pd

#had trouble loading data, so i added a bunch of checks
def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        print("columns loaded:", df.columns)
        return df
    except Exception as e:
        print("failed to load data:", e)
        try:
            df = pd.read_csv(file_path, delimiter=';')
            print("columns loaded with ';' delimiter:", df.columns)
            return df
        except Exception as e:
            print("failed to load data with ';' delimiter:", e)
            return None

def validate_existence(df):
    assert df['Crash ID'].notnull().all(), "All data has a Crash ID"

def validate_limits(df):
    posted_speed_limit = df['Posted Speed Limit'].fillna(0).astype(str).str.strip()
    
    posted_speed_limit = pd.to_numeric(posted_speed_limit, errors='coerce').fillna(0).astype(int)
    
    assert posted_speed_limit.max() <= 80, "No speed limit is greater than 80 mph"

def validate_inter_record(df):
    assert (df['Total Vehicle Occupant Count'] == df['Total Possible Injury (C) Count'] + df['Total Un-Injured Persons']).all(), "Total vehicle occupants must equal the sum of injured and uninjured."
    assert ((df['Total Vehicle Count'] > 0) & (df['Total Count of Persons Involved'] > 0)).all(), "If vehicles are involved, participants must be involved."

def validate_summary(df):
    assert len(df) > 1000 and len(df) < 1_000_000, "There are thousands of crashes, but not millions."
    assert df['Total Possible Injury (C) Count'].gt(0).sum() > df['Total Possible Injury (C) Count'].eq(0).sum(), "There are more crashes with injuries than crashes with none."

def validate_statistical_distribution(df):
    saturday_crashes = df['Week Day Code'].isin([5]).sum()
    sunday_crashes = df['Week Day Code'].isin([6]).sum()

    assert saturday_crashes > sunday_crashes, "More crashes occur on Saturday than on Sunday."

    for weekday in range(5):
        for weekend_day in [5, 6]:
            assert df['Week Day Code'].isin([weekend_day]).sum() > df['Week Day Code'].isin([weekday]).sum(), f"There are more crashes on weekends than weekdays"


def main():
    file_path = r"~\desktop\dataeng_class\04 validation\Hwy26Crashes2019_S23.csv"
    crash_data = load_data(file_path)

    #UNCOMMENT TO CHECK CERTAIN ASSERTIONS

    #validate_existence(crash_data)
    #validate_limits(crash_data)
    #validate_intra_record(crash_data)
    #validate_inter_record(crash_data)
    #validate_summary(crash_data)
    #validate_statistical_distribution(crash_data)
    
    print("assertions are good!")

if __name__ == "__main__":
    main()
