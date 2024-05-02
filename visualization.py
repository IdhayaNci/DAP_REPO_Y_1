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
    
    postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/dap"

    # A query to return all columns from unemployment_quality_of_life_cost_of_life Table
    query_string = """SELECT * FROM unemployment_quality_of_life_cost_full;"""

    try:
        # Connect to the PostgreSQL database
        engine = create_engine(postgres_connection_string)     
        # Run the query and return the results as a data frame
        with engine.connect() as connection:
            cost_of_living_dataframe = sqlio.read_sql_query(
                text(query_string), 
                connection
            )
            
        TOOLS = """hover,crosshair,pan,wheel_zoom,zoom_in,zoom_out,
            box_zoom,reset,tap,save,box_select,poly_select,
            lasso_select"""
        # Create a Bokeh figure
        p = figure(
            title="Total Cost vs UnEmployment",
            x_axis_label="UnEmployment",
            y_axis_label="Total Cost of Living",
            tools = TOOLS
        )
        # Create a scatter plot
        p.scatter(
            # the x variable
            cost_of_living_dataframe["population"],
            # the y variable
            cost_of_living_dataframe["total_cost"]
        )
        # show the visualisation
        show(p)
        return True
    except exc.SQLAlchemyError as dbError:
        print ("PostgreSQL Error", dbError)
    finally:
        if engine in locals(): 
            engine.close()