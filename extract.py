import json
import pandas as pd
from cassandra.cluster import Cluster
from dagster import op, Out, In, get_dagster_logger
from datetime import date, datetime
from pymongo import MongoClient, errors
import numpy as np

mongo_connection_string = "mongodb://127.0.0.1:27017"

logger = get_dagster_logger()


@op(
    out=Out(bool)
)

def extract_unemployment() -> bool:
    result = True
    try:
        print('hello world!')
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False
    
    # Return a Boolean indicating success or failure
    return result

@op(
    out=Out(bool)
)

def extract_quality_of_life() -> bool:
    result = True
    try:
        # Connect to the MongoDB database
        print('Hello World')
                
    # Trap and handle other errors
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False
    
    # Return a Boolean indicating success or failure
    return result


@op(
    out=Out(bool)
)

def extract_cost_of_living() -> bool:
    result = False
    try:
        print('Hello world')
        
    
           
    # Trap and handle errors
    except Exception as err:
        logger.error("Error: %s" % err)
    
    # If the insert was successful then set the result flag to True
    else:
        result = True
    
    # Return the result flag
    return(result)