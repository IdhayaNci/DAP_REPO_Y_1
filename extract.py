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
        # Connect to the MongoDB database
        client = MongoClient(mongo_connection_string)
        
        # Connect to the unemployment database
        unemployment_db = client["unemployment"]
        
        # Connect to the unemployment collection
        unemployment_collection = unemployment_db["unemployment"]
    
        # Open the file containing the data
        with open("unemployment_in_US_per_US_state.json","r") as fh:
            # Load the JSON data from file
            data = json.load(fh)
        for unemployment in data:
            try:
                    # Create a key for the MongoDB collection. This 
                    # ensures that we cannot have duplicate documents
                    key="{} {} {}".format(unemployment["year"],unemployment["month"],unemployment["state"]["area"])
                    unemployment["_id"] = key
                    # Insert the unemployment data as a document in the unemployment 
                    # collection
                    unemployment_collection.insert_one(unemployment)
                # Trap and handle duplicate key errors
            except errors.DuplicateKeyError as err:
                logger.error("Error: %s" % err)
                continue
                
    # Trap and handle other errors
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
        client = MongoClient(mongo_connection_string)
        
        # Connect to the quality of life database
        quality_of_life_db = client["quality_of_life"]
        
        # Connect to the quality of life collection
        quality_of_life_collection = quality_of_life_db["quality_of_life"]
    
        # Open the file containing the data
        with open("qol.json","r") as ql:
            # Load the JSON data from the file
            data = json.load(ql)
        for quality_of_life in data:
            try:
                    # Create a key for the MongoDB collection. This 
                    # ensures that we cannot have duplicate documents
                    key="{} {} {}".format(quality_of_life["NMCNTY"],quality_of_life["FIPS"],quality_of_life["LZIP"])
                    quality_of_life["_id"] = key
                    # Insert the quality of life data as a document in the quality of life collection
                    quality_of_life_collection.insert_one(quality_of_life)
                # Trap and handle duplicate key errors
            except errors.DuplicateKeyError as err:
                logger.error("Error: %s" % err)
                continue
                
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
         
        # Load the cost_of_living data into a Pandas data frame
        cost_of_living_df = pd.read_csv("cost_of_living_us_dap2.xls")
        cost_of_living_df.drop(columns=['isMetro'], inplace=True)
        # Set the column names for the data frame
        cost_of_living_df.columns = ["case_id","state","areaname","county","housing_cost","food_cost","transportation_cost","healthcare_cost","other_necessities_cost","childcare_cost","taxes","total_cost","median_family_income"]
        # Connect to the Cassandra database
        cassandra = Cluster(["127.0.0.1"])
        # Connect to the default keyspace
        cassandra_session = cassandra.connect()
        # Create the cost_of_living keyspace if it doesn't exist
        cassandra_session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS cost_of_living 
            WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};
            """
        )
        # against the cost_of_living keyspace
        cassandra_session.execute(
            """
            USE cost_of_living;
            """
        )

    
        # Create the cost_of_living table if it does not exist
        cassandra_session.execute(
            """
            CREATE TABLE IF NOT EXISTS cost_of_living(
            id int,
            case_id int,
            state Text,
            area_name Text,
            county Text,
            housing_cost float,
            food_cost float,
            transportation_cost float,
            healthcare_cost float,
            other_necessities_cost float,
            childcare_cost float,
            taxes float,
            total_cost float,
            median_family_income float,
            PRIMARY KEY(id)
            )
            """
        )
        # Create a format string for inserting data
        #  
        insert_string = """INSERT INTO cost_of_living (id,case_id,state,area_name,county,housing_cost,food_cost,transportation_cost,healthcare_cost,other_necessities_cost,childcare_cost,taxes,total_cost,median_family_income) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""


        for index, row in cost_of_living_df.iterrows():
            # Convert the row values to a list
            array = []
            array.append(index)
            
            row_values = row.values.flatten().tolist()
            for ris in row_values:
                array.append(ris)

            cassandra_session.execute(insert_string, array)
    except Exception as err:
        logger.error("Error: %s" % err)
    
    # If the insert was successful then set the result flag to True
    else:
        result = True
    
    # Return the result flag
    return(result)