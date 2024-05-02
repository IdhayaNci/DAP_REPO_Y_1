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

postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/dap"
mongo_connection_string = "mongodb://127.0.0.1:27017"
my_dict = {'AL':'Alabama', 'AK':'Alaska', 'AZ':	'Arizona', 'AR': 'Arkansas', 'CA' : 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware','DC': 'District of Columbia', 'FL': 'Florida','GA': 'Georgia', 'HI': 'Hawaii','ID': 'Idaho','IL':'Illinois', 'IN': 'Indiana','IA': 'Iowa', 'KS':'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana','ME':'Maine', 'MD':'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan','MN':'Minnesota', 'MS': 'Mississippi', 'MO':'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada','NH': 'New Hampshire','NJ': 'New Jersey', 'NM': 'New Mexico','NY': 'New York', 'NC': 'North Carolina','ND': 'North Dakota', 'OH':'Ohio','OK': 'Oklahoma','OR':'Oregon','PA': 'Pennsylvania', 'RI':'Rhode Island', 'SC': 'South Carolina','SD': 'South Dakota', 'TN': 'Tennessee','TX':'Texas','UT': 'Utah','VT': 'Vermont','VA': 'Virginia','WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming', 'PR': 'Puerto Rico', 'VI': 'Virgin Islands', 'GU': 'Guam', 'AS': 'American Samoa', 'MP' : 'Northern Marianas'}
engine = create_engine(postgres_connection_string)
logger = get_dagster_logger()


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

@op(
    ins={"start": In(bool)},
    out=Out(CostOfLivivngDataFrame)
)

def transform_cost_of_living(start):
    # Connect to the Cassandra database
    cassandra = Cluster(["127.0.0.1"])
    # Connect to the weather keyspace
    cassandra_session = cassandra.connect("cost_of_living")
    # Retrieve all rows in the weather table
    cost_of_living_df = pd.DataFrame(list(
        cassandra_session.execute("SELECT * FROM cost_of_living;")
    ))
    # print()
    keys = my_dict.keys()
    key_list = list(keys)

    for key in key_list:
        cost_of_living_df['state'] = cost_of_living_df['state'].replace(key, my_dict[key])

    cost_of_living_df['county'] = cost_of_living_df['county'].str.split('_', expand=True)[0]
    cost_of_living_df['area_name'] = cost_of_living_df['area_name'].str.split('_', expand=True)[0]
    cost_of_living_df['median_family_income'] = cost_of_living_df['median_family_income'].fillna(0)
    cost_of_living_df = cost_of_living_df.drop_duplicates(subset=['county'], keep='first')
    # cost_of_living_df.to_excel("cost_of_living_df.xlsx") 
    cost_of_living_df.to_sql('cost_of_living_structured', engine, if_exists='replace', index=False)
    return(cost_of_living_df)

def transform_quality_of_life(start):
    # Connect to the MongoDB database
    client = MongoClient(mongo_connection_string)
    # Connect to the flights collection
    quality_of_life_db = client["quality_of_life"]
    # Retrieve the data from the collection, flatten it and
    # return a Pandas data frame
    quality_of_life_df = pd.json_normalize(list(quality_of_life_db.quality_of_life.find({})))
    # Create a dictionary with column names as the key and the object (string) 
    # type as the value. This will be used to specify data types for the
    # data frame. We will change some of these types later.
    
    keys = my_dict.keys()
    key_list = list(keys)

    for key in key_list:
        quality_of_life_df['LSTATE'] = quality_of_life_df['LSTATE'].replace(key, my_dict[key])
    quality_of_life_df = quality_of_life_df.rename(columns={'LSTATE': 'state', 'NMCNTY': 'city', 'FIPS': 'fips', 'LZIP': 'zip', '2022 Population': 'population', '2016 Crime Rate': 'crime_rate', 'Unemployment': 'Unemployment','Diversity Rank (Race)': 'diversity_rank_race','Diversity Rank (Gender)': 'diversity_rank_gender' })
    required_columns = ['countyhelper', 'state', 'city', 'fips', 'zip', 'population', 'crime_rate', 'Unemployment', 'diversity_rank_race', 'diversity_rank_gender']
    quality_of_life_sub_df = quality_of_life_df[required_columns]
    quality_of_life_sub_df["crime_rate"] = ((quality_of_life_sub_df["crime_rate"].str.split('/', expand=True)[0].astype(int)) / 1000)
    quality_of_life_datatypes = dict(zip(quality_of_life_sub_df.columns, [object]*len(quality_of_life_sub_df.columns)))

    # quality_of_life_sub_df['crime_rate'] = quality_of_life_sub_df['crime_rate'].astype(int)
    

    # quality_of_life_sub_df = quality_of_life_sub_df.astype(quality_of_life_datatypes)
    quality_of_life_datatypes = dict(
        zip(quality_of_life_sub_df.columns, [object]*len(quality_of_life_sub_df.columns))
    )

    for column in ["fips", "zip","diversity_rank_race","diversity_rank_gender"]:
        quality_of_life_datatypes[column] = np.int32

    for column in ["crime_rate"]:
        quality_of_life_datatypes[column] = np.float64

    quality_of_life_sub_df = quality_of_life_sub_df.astype(quality_of_life_datatypes)
    quality_of_life_sub_df = quality_of_life_sub_df.drop_duplicates(subset=['city'], keep='first')
    print(len(quality_of_life_sub_df), 'quality_of_life_sub_df')
    # quality_of_life_sub_df.drop(["_id"],axis=1,inplace=True)
    # quality_of_life_sub_df.to_excel("quality_of_life.xlsx") 
    quality_of_life_sub_df.to_sql('quality_of_life_structured', engine, if_exists='replace', index=False)
    return quality_of_life_sub_df





@op(
    ins={"start": In(bool)},
    out=Out(UnemploymentDataFrame)
)

def transform_unemployment(start):
 
    client = MongoClient(mongo_connection_string)
  
    unemployment_db = client["unemployment"]

    unemployment_df = pd.json_normalize(list(unemployment_db.unemployment.find({})))

    unemployment_datatypes = dict(
        zip(unemployment_df.columns, [object]*len(unemployment_df.columns))
    )
    unemployment_df["fips_code"] = unemployment_df['fips_code'].astype(int)
    unemployment_df['state'] = unemployment_df['state.area']
    for column in ["percent_of_state.area_population","percent_of_labor_force_employed_in_state.area","percent_of_labor_force_unemployed_in_state.area"]:
        unemployment_datatypes[column] = np.float64


    for columns in ["total_civilian_non_institutional_population_in_state.area", "total_civilian_labor_force_in_state.area", "total_employment_in_state.area", "total_unemployment_in_state.area"]:
        unemployment_df[columns] = unemployment_df[columns].str.replace(',', '').str.strip()
        unemployment_df[columns] = unemployment_df[columns].astype(int)
    
    unemployment_df = unemployment_df.astype(unemployment_datatypes)
    unemployment_df = unemployment_df[unemployment_df['year'] > 2010]


    unemployment_df.drop(["state.area"],axis=1,inplace=True)


    
    unemployment_df = unemployment_df.drop_duplicates(subset=['state'], keep='first')

    unemployment_df.to_sql('unemployment_structured', engine, if_exists='replace', index=False)
    # Return the flights data frame    
    return unemployment_df

@op(
    ins={"unemployment_df": In(pd.DataFrame), "cost_of_living_df": In(CostOfLivivngDataFrame), 'quality_of_life_sub_df': In(pd.DataFrame)},
    out=Out(pd.DataFrame)
)

def join(unemployment_df,cost_of_living_df,quality_of_life_sub_df) -> pd.DataFrame:
    # engine = create_engine(postgres_connection_string)
    common_column = 'state'
    query = """ SELECT * FROM cost_of_living_structured AS cost 
    INNER JOIN unemployment_structured AS unemploy ON cost.{0} = unemploy.{0}
    INNER JOIN quality_of_life_structured AS quality ON cost.{0} = quality.{0};""".format(common_column)
    merged_df = pd.DataFrame()
    merged_quality_df = pd.DataFrame()
    # engine = create_engine(postgres_connection_string)
    with engine.connect() as connection:
        merged_df = sqlio.read_sql_query(text(query),connection)
        required_columns = ['id', 'area_name', 'case_id', 'childcare_cost', 'county', 'food_cost','healthcare_cost', 'housing_cost', 'median_family_income', 'other_necessities_cost','taxes', 'total_cost', 'transportation_cost', '_id', 'fips_code', 'year', 'month', 'total_civilian_non_institutional_population_in_state.area', 'total_civilian_labor_force_in_state.area', 'percent_of_state.area_population', 'total_employment_in_state.area',
       'percent_of_labor_force_employed_in_state.area','total_unemployment_in_state.area', 'percent_of_labor_force_unemployed_in_state.area', 'state', 'countyhelper', 'city', 'fips', 'zip', 'population',
       'crime_rate', 'Unemployment', 'diversity_rank_race', 'diversity_rank_gender']
        merged_quality_df = merged_df[required_columns]

        merged_quality_df = merged_quality_df.loc[:, ~merged_quality_df.columns.duplicated()]

    return(merged_quality_df)

    

@op(
    ins={"start": In(pd.DataFrame)},
    out=Out(bool)
)

def load(start):
    try:
        
        with engine.connect() as conn:

            logger.info("{} records loaded".format(0))
            
        engine.dispose(close=True)
        
        return 0 > 0
    
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False