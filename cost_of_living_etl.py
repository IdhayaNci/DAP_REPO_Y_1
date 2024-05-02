from dagster import job
from extract import *
from transfer_and_load import *
from visualization import *
from model import *
@job
def etl():# ETL function
    # Feed the joined data into model
    model(
        # Visualize the joined data
        visualise(
        # Load the joined data into PostgreSQL
            load(
             # Join the three data
                join(
                # Transform the stored unemployment data
                    transform_unemployment(
                    # Extract and store the unemployment data
                        extract_unemployment()
                    ),
                # Transform the stored cost_of_living data
                    transform_cost_of_living(
                    # Extract and store the cost_of_living data
                        extract_cost_of_living()
                    ),
                # Transform the stored quality_of_life data
                    transform_quality_of_life(
                    # Extract and store the quality_of_life data
                        extract_quality_of_life()
                    )
                )
            )
        )
    )