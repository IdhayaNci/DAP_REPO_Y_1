import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import GridSearchCV
import pandas.io.sql as sqlio
from sqlalchemy import create_engine, event, text, exc
from dagster import op, In

@op(
    ins={"start": In(bool)}
)
def model(start):
    # PostgreSQL URI for Docker Instance
    postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/dap"

    # A query to return all columns from unemployment_quality_of_life_cost_of_life Table
    query_string = """SELECT * FROM unemployment_quality_of_life_cost_full;"""

    try:
        # Connect to the PostgreSQL database
        engine = create_engine(postgres_connection_string)     
        # Run the query and return the results as a data frame
        with engine.connect() as connection:
            cost_of_living_us_dataframe = sqlio.read_sql_query(
                text(query_string), 
                connection
            )
            threshold = cost_of_living_us_dataframe['total_cost'].median()
            cost_of_living_us_dataframe['Cost_of_Living_Category'] = (cost_of_living_us_dataframe['total_cost'] > threshold).astype(int)
            # Extract features  variable
            X = cost_of_living_us_dataframe[['childcare_cost', 'food_cost', 'healthcare_cost', 'housing_cost','median_family_income', 'other_necessities_cost', 'taxes','transportation_cost', 'total_civilian_non_institutional_population_in_state.area','total_civilian_labor_force_in_state.area', 'percent_of_state.area_population','total_employment_in_state.area', 'percent_of_labor_force_employed_in_state.area','total_unemployment_in_state.area', 'percent_of_labor_force_unemployed_in_state.area','crime_rate', 'Unemployment', 'diversity_rank_race', 'diversity_rank_gender']]
            # Extract target  variable
            y = cost_of_living_us_dataframe['Cost_of_Living_Category']

            # Split data into train and test sets
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

            # Initialize Random Forest classifier
            clf = RandomForestClassifier()

            # Train the model
            clf.fit(X_train, y_train)

            # Make predictions on the test set
            y_pred = clf.predict(X_test)

            # Evaluate the model
            accuracy = accuracy_score(y_test, y_pred)
            print("Accuracy:", accuracy)
            print(classification_report(y_test, y_pred))
            print_statement = classification_report(y_test, y_pred)
            feature_importances = clf.feature_importances_

            # Create a DataFrame to display feature importances
            feature_importance_df = pd.DataFrame({'Feature': X.columns, 'Importance': feature_importances})
            feature_importance_df = feature_importance_df.sort_values(by='Importance', ascending=False)

            print("\nFeature Importance:")
            print(feature_importance_df)
            html_output = "<p>{}</p>".format(print_statement)

            # Write the HTML content to a file or display it
            # with open("print_to_html.html", "w") as html_file: 
            #     html_file.write(html_output)
            return True
    except exc.SQLAlchemyError as dbError:
        print ("PostgreSQL Error", dbError)
    finally:
        if engine in locals(): 
            engine.close()
