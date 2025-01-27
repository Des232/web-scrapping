import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists

# Load the CSV file
df = pd.read_csv('RewardsData.csv')

# a. Remove the 'Tags' column
df = df.drop(columns=['Tags'])

# b. Arrange in alphabetical order all the unique values in the 'City' column
unique_cities = sorted(df['City'].dropna().unique())

# c. Replace all formats of Winston-salem with "Winston-Salem"
df['City'] = df['City'].str.replace('Winston-salem', 'Winston-Salem', case=False)

# d. Replace all city names with the correct format
df['City'] = df['City'].str.title()

# e. Repeat steps a to d for the 'City' and 'State' columns
df['State'] = df['State'].str.title()

# f. Replace all state abbreviations with the full state names
state_abbreviations = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts',
    'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana',
    'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico',
    'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota',
    'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington',
    'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'
}

df['State'] = df['State'].replace(state_abbreviations)

# g. Fill empty cells in 'State' column with state names in alphabetical order
states = sorted(state_abbreviations.values())
state_cycle = iter(states)
df['State'] = df['State'].fillna(lambda _: next(state_cycle))

# h. Truncate zip codes longer than 5 digits
df['Zip'] = df['Zip'].astype(str).str[:5]

# i. Drop rows with zip codes less than 5 digits
df = df[df['Zip'].str.len() == 5]

# j. Save the resulting dataframe in a PostgreSQL database
database_url = 'postgresql+psycopg2://postgres:postgres@localhost:5432/rewards_data'
engine = create_engine(database_url)

if not database_exists(database_url):
    create_database(database_url)
table_name = 'cleaned_rewards_data'
# k. Create the database if it does not exist
# l. Replace the contents of the existing table if it already exists
df.to_sql('table_name',con= engine, if_exists='replace', index=False)





# import pandas as pd
# from sqlalchemy import create_engine
# from sqlalchemy_utils import database_exists, create_database
# import os

# # Load the uploaded CSV file
# os.chdir(r"C:\Users\MTECH\Desktop\Python Exam Codes")
# rewards_data = pd.read_csv('RewardsData.csv')
# rewards_data["Birthdate"]=pd.to_datetime(rewards_data["Birthdate"])
# print(rewards_data.to_string())
# # Step a: Remove the 'Tags' column
# rewards_data = rewards_data.drop(columns=['Tags'])

# # Step b: Arrange in alphabetical order all unique values in the 'City' column
# unique_cities = sorted(rewards_data['City'].dropna().unique())

# # Step c: Replace all formats of Winston-salem with "Winston-Salem"
# rewards_data['City'] = rewards_data['City'].replace(
#     to_replace=r'(?i)winston[-\s]*salem', value="Winston-Salem", regex=True
# )

# # Step d: Correct city names to proper case
# rewards_data['City'] = rewards_data['City'].str.title()

# # Repeat steps a to d for 'City' and 'State' columns
# # Correct state names to proper case
# #Step e:
# rewards_data['State'] = rewards_data['State'].str.title()

# # Step f: Replace all state abbreviations with full names
# state_abbreviation_to_full = {
#     'AL': 'Alabama','Al':"Alabama", 'AK': 'Alaska', 'AZ': 'Arizona', "Az":"Arizona",'AR': 'Arkansas', 
#     'CA': 'California', "Ca":"California",'CO': 'Colorado', "Co":"Colorado",'CT': 'Connecticut', "Ct":"Connecticut",
#     'DE': 'Delaware', 'FL': 'Florida',"Fl":"Florida", 'GA': 'Georgia', "Ga":"Georgia",
#     'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', "Il":"Illionois",
#     'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas', 
#     'KY': 'Kentucky',"Ky":"Kentucky",'LA': 'Louisiana', 'ME': 'Maine', 
#     'MD': 'Maryland', "Md":"Maryland", "MA": 'Massachusetts', 'MI': 'Michigan', 
#     'MN': 'Minnesota','Mn':'Minnesota', 'MS': 'Mississippi','Ms':'Mississippi', 'MO': 'Missouri', 
#     'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 
#     'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 
#     'NY': 'New York',"Ny":"New York",'Nc':'North Carolina', 'NC': 'North Carolina', 'ND': 'North Dakota', 
#     'OH': 'Ohio', "Oh":"Ohio",'OK': 'Oklahoma', 'OR': 'Oregon', 
#     'PA': 'Pennsylvania',"Pa":"Pennysylvania", 'RI': 'Rhode Island', 'SC': 'South Carolina', "Sc":"South Carolina",
#     'SD': 'South Dakota', 'TN': 'Tennessee','Tn':'Tennessee','TX': 'Texas', "Tx":"Texas",
#     'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'Va':'Virginia',
#     'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', "Wi":"Wisconsin",
#     'WY': 'Wyoming'
# }
# rewards_data['State'] = rewards_data['State'].replace(state_abbreviation_to_full)

# # Step g: Fill empty 'State' cells with states in alphabetical order
# states_full_list = sorted(state_abbreviation_to_full.values())
# nan_state_count = rewards_data['State'].isna().sum()
# states_cycle = (states_full_list * (nan_state_count // len(states_full_list) + 1))[:nan_state_count]
# rewards_data.loc[rewards_data['State'].isna(), 'State'] = states_cycle

# # Step h: Truncate 'Zip' codes longer than 5 digits
# rewards_data['Zip'] = rewards_data['Zip'].astype(str).str[:5]

# # Step i: Drop rows with 'Zip' codes less than 5 digits
# rewards_data = rewards_data[rewards_data['Zip'].str.len() == 5]
# print(rewards_data)


# #Step J:
# # Saving to a new Database 
# # Datacredentials and connection
# uid = 'postgres'
# pwd = ''
# postgres_url = f'postgresql+psycopg2://{uid}:{pwd}@localhost:5432/Rewards_Data'

# # Create the database if it does not exist
# if not database_exists(postgres_url):
#     create_database(postgres_url)
#     print("Database created")
# else:
#     print("Database exists")

# engine = create_engine(postgres_url)

# #Save data to the database
# table_name = 'RewardsDatabase'
# rewards_data.to_sql(table_name, engine, if_exists='replace', index=False)
