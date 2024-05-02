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

def classification_model(start):
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
            print('Here Model')
            threshold = cost_of_living_us_dataframe['total_cost'].median()
            cost_of_living_us_dataframe['Cost_of_Living_Category'] = (cost_of_living_us_dataframe['total_cost'] > threshold).astype(int)
# Extract features and target variable
        # X = cost_of_living_us_dataframe[['taxes', 'total_unemployment_in_state.area', 'crime_rate']]
        # y = cost_of_living_us_dataframe['total_cost']
            X = cost_of_living_us_dataframe[['total_unemployment_in_state.area', 'crime_rate', 'taxes']]
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
    except exc.SQLAlchemyError as dbError:
        print ("PostgreSQL Error", dbError)
    finally:
        if engine in locals(): 
            engine.close()
    
# Separate features (X) and target variable (y)


# Split the data into training and testing sets


# Standardize features


# Train a Random Forest classifier


# Print the best parameters found by grid search


# Evaluate the model


# Classification report

