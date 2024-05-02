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

@op(
    ins={"start": In(bool)},
    out=Out([])
)

def transform_cost_of_living(start):
    
    return([])

@op(
    ins={"start": In(bool)},
    out=Out([])
)

def transform_quality_of_life(start):
   return []



@op(
    ins={"start": In(bool)},
    out=Out([])
)

def transform_unemployment(start):
 
    
    # Return the flights data frame    
    return []


@op(
    ins={"dataframe_1": In(pd.DataFrame), "dataframe_2": In([]), 'dataframe_3': In(pd.DataFrame)},
    out=Out(pd.DataFrame)
)

def join(unemployment_df,cost_of_living_df,quality_of_life_sub_df) -> pd.DataFrame:
    

    return []

    

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