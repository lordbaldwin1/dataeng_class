import pandas as pd

census_data = pd.read_csv('acs2017_census_tract_data.csv')
census_data_copy = census_data.copy()

census_data_copy['County'] = census_data_copy['County'].str.replace(' County', '')

county_aggregated_data = census_data_copy.groupby(['State', 'County']).agg({
    'TotalPop': 'sum',
    'Poverty': 'mean',
    'IncomePerCap': 'mean'
}).reset_index()

county_aggregated_data['ID'] = range(1, len(county_aggregated_data) + 1)

covid_data = pd.read_csv('COVID_county_data.csv')

covid_data['date'] = pd.to_datetime(covid_data['date'])
covid_data['Month'] = covid_data['date'].dt.month
covid_data['Year'] = covid_data['date'].dt.year

covid_data = covid_data.merge(county_aggregated_data[['State', 'County', 'ID']], left_on=['state', 'county'], right_on=['State', 'County'], how='left')

covid_monthly_agg = covid_data.groupby(['ID', 'Year', 'Month']).agg({
    'cases': 'max',
    'deaths': 'max'
}).reset_index()

total_cases_deaths = covid_monthly_agg.groupby('ID').agg({
    'cases': 'sum',
    'deaths': 'sum'
}).reset_index()

total_cases_deaths.columns = ['ID', 'TotalCases', 'TotalDeaths']

covid_summary = county_aggregated_data[['ID', 'State', 'County', 'TotalPop', 'Poverty', 'IncomePerCap']].merge(total_cases_deaths, on='ID', how='left').fillna(0)

covid_summary['TotalCasesPer100K'] = covid_summary['TotalCases'] / (covid_summary['TotalPop'] / 100000)
covid_summary['TotalDeathsPer100K'] = covid_summary['TotalDeaths'] / (covid_summary['TotalPop'] / 100000)

covid_summary = covid_summary[['ID', 'State', 'County', 'TotalPop', 'Poverty', 'IncomePerCap', 'TotalCases', 'TotalDeaths', 'TotalCasesPer100K', 'TotalDeathsPer100K']]

specified_counties = [{'State': 'Oregon', 'County': 'Washington'},{'State': 'Oregon', 'County': 'Malheur'},{'State': 'Virginia', 'County': 'Loudoun'},{'State': 'Kentucky', 'County': 'Harlan'}]

print(f"{'County'} {'%Poverty'} {'TotalCasesPer100K'}")
for county in specified_counties:
    result = covid_summary[(covid_summary['State'] == county['State']) & 
                           (covid_summary['County'] == county['County'])]
    if not result.empty:
        county_name = f"{county['County']}"
        poverty = result['Poverty'].values[0]
        total_cases_per_100k = result['TotalCasesPer100K'].values[0]
        print(f"{county_name} {poverty} {total_cases_per_100k}")
    else:
        print(f"{county['County']} County, {county['State']} data not found.")
