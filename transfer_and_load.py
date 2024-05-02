import numpy as np
import pandas as pd
from cassandra.cluster import Cluster
from dagster import op, Out, In, get_dagster_logger
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from pymongo import MongoClient, errors
from sqlalchemy import create_engine, exc, VARCHAR, DECIMAL, INT, text
from sqlalchemy.pool import NullPool
from sqlalchemy.types import *
import pandas.io.sql as sqlio

# PostgreSQL URI for Docker Instance
postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/dap"
# MongoDB URI for Docker Instance
mongo_connection_string = "mongodb://127.0.0.1:27017"
my_dict = {'AL':'Alabama', 'AK':'Alaska', 'AZ':	'Arizona', 'AR': 'Arkansas', 'CA' : 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware','DC': 'District of Columbia', 'FL': 'Florida','GA': 'Georgia', 'HI': 'Hawaii','ID': 'Idaho','IL':'Illinois', 'IN': 'Indiana','IA': 'Iowa', 'KS':'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana','ME':'Maine', 'MD':'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan','MN':'Minnesota', 'MS': 'Mississippi', 'MO':'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada','NH': 'New Hampshire','NJ': 'New Jersey', 'NM': 'New Mexico','NY': 'New York', 'NC': 'North Carolina','ND': 'North Dakota', 'OH':'Ohio','OK': 'Oklahoma','OR':'Oregon','PA': 'Pennsylvania', 'RI':'Rhode Island', 'SC': 'South Carolina','SD': 'South Dakota', 'TN': 'Tennessee','TX':'Texas','UT': 'Utah','VT': 'Vermont','VA': 'Virginia','WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming', 'PR': 'Puerto Rico', 'VI': 'Virgin Islands', 'GU': 'Guam', 'AS': 'American Samoa', 'MP' : 'Northern Marianas'}
engine = create_engine(postgres_connection_string)

# To retrieve  a logger instance
logger = get_dagster_logger()

#custom Dagster type for unemployment Dataset
UnemploymentDataFrame = create_dagster_pandas_dataframe_type(
    name="UnemploymentDataFrame",
    columns=[
        PandasColumn.string_column(
            name="fips_code",
            non_nullable=True # specify that the column shouldn't contain NAs
        ),
        PandasColumn.string_column(
            name="state",
            non_nullable=True    
        ),
        PandasColumn.string_column(
            name="year",
            non_nullable=True    
        ),
        PandasColumn.string_column(
            name="month",
            non_nullable=True    
        ),
        PandasColumn.string_column(
            name="total_civilian_non_institutional_population_in_state.area",
            non_nullable=True    
        ),
        PandasColumn.string_column(
            name="total_civilian_labor_force_in_state.area",
            non_nullable=True
        ),
        PandasColumn.float_column(
            name="percent_of_state.area_population",
            non_nullable=True
        ),
        PandasColumn.string_column(
            name="total_employment_in_state.area",
            non_nullable=True
        ),
        PandasColumn.float_column(
            name="percent_of_labor_force_employed_in_state.area",
            non_nullable=True    
        ),
        PandasColumn.string_column(
            name="total_unemployment_in_state.area",
            non_nullable=True    
        ),
        PandasColumn.float_column(
            name="percent_of_labor_force_unemployed_in_state.area",
            non_nullable=True    
        )
    ]
)

#Dataframe TYPE check for Quality of life Dataset
QualityOfLifeDataFrame = create_dagster_pandas_dataframe_type(
    name="QualityOfLifeDataFrame",
    columns=[
        PandasColumn.string_column(
            name="countyhelper",
            non_nullable=True # specify that the column shouldn't contain NAs
        ),
        PandasColumn.string_column(
            name="state",
            non_nullable=True    
        ),
        PandasColumn.string_column(
            name="city",
            non_nullable=True    
        ),
        PandasColumn.numeric_column(
            name="fips",
            non_nullable=True    
        ),
        PandasColumn.numeric_column(
            name="zip",
            non_nullable=True    
        ),
        PandasColumn.string_column(
            name="population",
            non_nullable=True
        ),
        PandasColumn.float_column(
            name="crime_rate",
            non_nullable=True
        ),
        PandasColumn.string_column(
            name="Unemployment",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="diversity_rank_race",
            non_nullable=True    
        ),
        PandasColumn.numeric_column(
            name="diversity_rank_gender",
            non_nullable=True    
        )
    ]
)

#custom Dagster type for Cost of life Dataset
CostOfLivivngDataFrame = create_dagster_pandas_dataframe_type(
    name="CostOfLivivngDataFrame",
    columns=[
        PandasColumn.numeric_column(
            name="id",
            non_nullable=True # specify that the column shouldn't contain NAs
        ),
        PandasColumn.numeric_column(
            name="case_id",
            non_nullable=True
        ),
        PandasColumn.string_column(
            name="state",
            non_nullable=True
        ),
        PandasColumn.string_column(
            name="area_name",
            non_nullable=True
        ),
        PandasColumn.string_column(
            name="county",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="housing_cost",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="food_cost",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="transportation_cost",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="healthcare_cost",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="other_necessities_cost",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="childcare_cost",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="taxes",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="total_cost",
            non_nullable=True
        ),
        PandasColumn.numeric_column(
            name="median_family_income",
            non_nullable=True
        )
    ]
)

@op(
    ins={"start": In(bool)},
    out=Out(CostOfLivivngDataFrame)
)

def transform_cost_of_living(start):
    # Connect to the Cassandra database
    cassandra = Cluster(["127.0.0.1"])
    # Connect to the cost_of_living keyspace
    cassandra_session = cassandra.connect("cost_of_living")
    # Retrieve all rows in the cost_of_living table
    cost_of_living_df = pd.DataFrame(list(
        cassandra_session.execute("SELECT * FROM cost_of_living;")
    ))
    # get keys of the my_dict dictonaries
    keys = my_dict.keys()
    key_list = list(keys) # converts it into list

    for key in key_list:
        cost_of_living_df['state'] = cost_of_living_df['state'].replace(key, my_dict[key])  # replace key with dictionary value

    cost_of_living_df['county'] = cost_of_living_df['county'].str.split('_', expand=True)[0]# split string using '_'
    cost_of_living_df['area_name'] = cost_of_living_df['area_name'].str.split('_', expand=True)[0]
    cost_of_living_df['median_family_income'] = cost_of_living_df['median_family_income'].fillna(0) # fill nan value with 0 
    cost_of_living_df = cost_of_living_df.drop_duplicates(subset=['county'], keep='first') # remove duplicates 
    
    cost_of_living_df.to_sql('cost_of_living_structured', engine, if_exists='replace', index=False) # To store cost of living dataframe in cost of living structured
    return(cost_of_living_df)

@op(
    ins={"start": In(bool)},
    out=Out(QualityOfLifeDataFrame)
)

def transform_quality_of_life(start):
    # Connect to the MongoDB database
    client = MongoClient(mongo_connection_string)
    # Connect to the quality_of_life collection
    quality_of_life_db = client["quality_of_life"]
    
    # return a Pandas data frame
    quality_of_life_df = pd.json_normalize(list(quality_of_life_db.quality_of_life.find({})))
   
     # get keys of the my_dict dictonaries
    keys = my_dict.keys()
    key_list = list(keys) # converts it into list

    for key in key_list:
        quality_of_life_df['LSTATE'] = quality_of_life_df['LSTATE'].replace(key, my_dict[key])# replace key with dictionary value
    quality_of_life_df = quality_of_life_df.rename(columns={'LSTATE': 'state', 'NMCNTY': 'city', 'FIPS': 'fips', 'LZIP': 'zip', '2022 Population': 'population', '2016 Crime Rate': 'crime_rate', 'Unemployment': 'Unemployment','Diversity Rank (Race)': 'diversity_rank_race','Diversity Rank (Gender)': 'diversity_rank_gender' })
    required_columns = ['countyhelper', 'state', 'city', 'fips', 'zip', 'population', 'crime_rate', 'Unemployment', 'diversity_rank_race', 'diversity_rank_gender']
    quality_of_life_sub_df = quality_of_life_df[required_columns]
    quality_of_life_sub_df["crime_rate"] = ((quality_of_life_sub_df["crime_rate"].str.split('/', expand=True)[0].astype(int)) / 1000) # split string using '/'
    quality_of_life_datatypes = dict(zip(quality_of_life_sub_df.columns, [object]*len(quality_of_life_sub_df.columns)))
    quality_of_life_sub_df['population'] = (quality_of_life_sub_df['population'].str.replace(',', '').str.strip()).astype(int)  # split string using ','
    quality_of_life_sub_df['Unemployment'] = quality_of_life_sub_df['Unemployment'].str.replace('%', '').str.strip()# split string using '%'
    quality_of_life_sub_df['Unemployment'] = ( (quality_of_life_sub_df['Unemployment'].astype(float)) * quality_of_life_sub_df['population'] ) / 100

    quality_of_life_datatypes = dict(
        zip(quality_of_life_sub_df.columns, [object]*len(quality_of_life_sub_df.columns))
    )

    for column in ["fips", "zip","diversity_rank_race","diversity_rank_gender"]:
        quality_of_life_datatypes[column] = np.int32 # int check for the above column

    for column in ["crime_rate"]:
        quality_of_life_datatypes[column] = np.float64 # float check for the above column

    quality_of_life_sub_df = quality_of_life_sub_df.astype(quality_of_life_datatypes)
    quality_of_life_sub_df = quality_of_life_sub_df.drop_duplicates(subset=['city'], keep='first') # To drop duplicate rows
    
    quality_of_life_sub_df.to_sql('quality_of_life_structured', engine, if_exists='replace', index=False)  # To store quality of life dataframe into database
    return quality_of_life_sub_df



@op(
    ins={"start": In(bool)},
    out=Out(UnemploymentDataFrame)
)

def transform_unemployment(start):
     # Connect to the MongoDB database
    client = MongoClient(mongo_connection_string)
    #Connect to the unemployment collection
    unemployment_db = client["unemployment"]
    # return a Pandas data frame    
    unemployment_df = pd.json_normalize(list(unemployment_db.unemployment.find({})))

    unemployment_datatypes = dict(
        zip(unemployment_df.columns, [object]*len(unemployment_df.columns))
    )
    unemployment_df["fips_code"] = unemployment_df['fips_code'].astype(int) # int check for fips column
    unemployment_df['state'] = unemployment_df['state.area'] # chaging column name for joining process
    for column in ["percent_of_state.area_population","percent_of_labor_force_employed_in_state.area","percent_of_labor_force_unemployed_in_state.area"]:
        unemployment_datatypes[column] = np.float64 # float check for the above column


    for columns in ["total_civilian_non_institutional_population_in_state.area", "total_civilian_labor_force_in_state.area", "total_employment_in_state.area", "total_unemployment_in_state.area"]:
        unemployment_df[columns] = unemployment_df[columns].str.replace(',', '').str.strip() # to replace ',' string with empty column
        unemployment_df[columns] = unemployment_df[columns].astype(int) # int check for above column
    
    unemployment_df = unemployment_df.astype(unemployment_datatypes)
    unemployment_df = unemployment_df[unemployment_df['year'] > 2010] # Since this dataset has more row and classification after 2010, condition s are created using pandas


    unemployment_df.drop(["state.area"],axis=1,inplace=True) # to drop state.area column from dataframe


    
    unemployment_df = unemployment_df.drop_duplicates(subset=['state'], keep='first') # To drop duplicate rows

    unemployment_df.to_sql('unemployment_structured', engine, if_exists='replace', index=False) # To store unemployment dataframe into database
    # Return the unemployment dataframe    
    return unemployment_df


@op(
    ins={"unemployment_df": In(pd.DataFrame), "cost_of_living_df": In(CostOfLivivngDataFrame), 'quality_of_life_sub_df': In(pd.DataFrame)},
    out=Out(pd.DataFrame)
)

def join(unemployment_df,cost_of_living_df,quality_of_life_sub_df) -> pd.DataFrame:
    #creating common column variable for query
    common_column = 'state'

    #Query to select all the columns from table and join all three table using common column 
    query = """ SELECT * FROM cost_of_living_structured AS cost 
    INNER JOIN unemployment_structured AS unemploy ON cost.{0} = unemploy.{0}
    INNER JOIN quality_of_life_structured AS quality ON cost.{0} = quality.{0};""".format(common_column)
    merged_df = pd.DataFrame() # To create empty dataframe
    merged_quality_df = pd.DataFrame()
    # engine = create_engine(postgres_connection_string)
    with engine.connect() as connection:
        merged_df = sqlio.read_sql_query(text(query),connection) 
        required_columns = ['id', 'area_name', 'case_id', 'childcare_cost', 'county', 'food_cost','healthcare_cost', 'housing_cost', 'median_family_income', 'other_necessities_cost','taxes', 'total_cost', 'transportation_cost', '_id', 'fips_code', 'year', 'month', 'total_civilian_non_institutional_population_in_state.area', 'total_civilian_labor_force_in_state.area', 'percent_of_state.area_population', 'total_employment_in_state.area',
       'percent_of_labor_force_employed_in_state.area','total_unemployment_in_state.area', 'percent_of_labor_force_unemployed_in_state.area', 'state', 'countyhelper', 'city', 'fips', 'zip', 'population',
       'crime_rate', 'Unemployment', 'diversity_rank_race', 'diversity_rank_gender']  # To get only required columns
        merged_quality_df = merged_df[required_columns] 

        merged_quality_df = merged_quality_df.loc[:, ~merged_quality_df.columns.duplicated()] # To remove duplicates

    return(merged_quality_df)

    

@op(
    ins={"merged_quality_df": In(pd.DataFrame)},
    out=Out(bool)
)

def load(merged_quality_df):
    try:

        database_datatypes = dict(zip(merged_quality_df.columns,[VARCHAR]*len(merged_quality_df.columns)))
        # Set columns with Float datatype
        for column in ["percent_of_state.area_population","percent_of_labor_force_employed_in_state.area","percent_of_labor_force_unemployed_in_state.area","case_id","housing_cost","food_cost","transportation_cost","healthcare_cost", "other_necessities_cost", "childcare_cost", "taxes", "total_cost", "median_family_income", "crime_rate"]:
            database_datatypes[column] = DECIMAL
        
        # Set columns with INT datatype
        for column in [ "fips", "zip", "diversity_rank_race", "diversity_rank_gender", "total_civilian_non_institutional_population_in_state.area", "total_civilian_labor_force_in_state.area", "total_employment_in_state.area", "total_unemployment_in_state.area"]:
            database_datatypes[column] = INT

        # Open the connection to the PostgreSQL server
        with engine.connect() as conn:
            # Store the data frame contents to the unemployment_quality_of_life_cost_full 
            # table, using the dictionary of data types created
            # above and replacing any existing table
            
            rowcount = merged_quality_df.to_sql(
                name="unemployment_quality_of_life_cost_full",
                con=engine,
                index=False,
                if_exists="replace"
            )
            
            logger.info("{} records loaded".format(rowcount))
            
        # Close the connection to PostgreSQL and dispose of 
        # the connection engine
        engine.dispose(close=True)
        
        # Return the number of rows inserted
        return rowcount > 0
    
    # Trap and handle any relevant errors
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False