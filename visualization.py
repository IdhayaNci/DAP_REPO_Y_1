import pandas.io.sql as sqlio
import psycopg2
from dagster import op, In
from bokeh.plotting import figure, show
from sqlalchemy import create_engine, event, text, exc
from sqlalchemy.engine.url import URL

@op(
    ins={"start": In(bool)}
)

def visualise(start):

    try:
        
        return True
    except exc.SQLAlchemyError as dbError:
        print ("PostgreSQL Error", dbError)
    finally:
        if engine in locals(): 
            engine.close()