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