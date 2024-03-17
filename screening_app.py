"""     This is The API for providing interface in accessing data     """

import re
from os import getenv
import psycopg2
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, Engine
from sqlalchemy import URL
from rapidfuzz import fuzz
from fastapi import FastAPI, Request
from fastapi.responses import ORJSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.gzip import GZipMiddleware

# Creatomg the Instance of a Screening API
screen_app: FastAPI = FastAPI()

# Front-end Design Stuff
screen_app.add_middleware(GZipMiddleware)
templates: Jinja2Templates = Jinja2Templates(directory="templates")

# Function for getting the table from Postgres
def get_consolidated_table() -> pd.DataFrame:
    """
    Summary:
        Function for building a sqlalchemy engine and loading the table from Postgres DB

    Returns:
        pd.DataFrame: Returns the DataFrame of the Consolidated Table from Postgres DB
    """
    # Get DB Variables
    pg_host = getenv("DB_HOST")
    pg_port = getenv("DB_PORT")
    pg_name = getenv("DB_NAME")
    pg_user = getenv("DB_USER")
    pg_key = getenv("DB_PASS")

    # Define a connection string to Postgres DB
    connection_str = URL.create(
        drivername="postgresql+psycopg2",
        username=pg_user,
        password=pg_key,
        host=pg_host,
        port=pg_port,
        database=pg_name
    )

    # Create an engine object
    engine = create_engine(url=connection_str)

    # Get table from DB
    df = pd.read_sql("SELECT * FROM ofac_consolidated", con=engine)

    return df

# Function for formatting informations from the DataFrame
def cleanning_names(names: str) -> str:
    """
    Summary: Function for Cleaning the names/information from a table

    Args:
        names (str): The Name or Information to be cleaned

    Returns:
        str: The clean and formatted version of the Argument
    """
    # Replaces /- with spaces and Converts to Uppercase
    clean_name = re.sub("[/-]", " ", names).upper()

    # Keep only alphanumeric and spaces, remove other characters
    clean_name = re.sub(r"[^\w\s]", "", clean_name)  # r prefix used for raw string

    # Remove consecutive spaces
    clean_name = re.sub("\\s+", " ", clean_name).strip()

    return clean_name

# Function for performing fuzzy matching
def get_ratio(s1: str, s2: str, sort_names: bool = False) -> float | None:
    """
    Summary:
        Function for getting the ratio of match between two 
        strings with an optional function of sorting.

    Args:
        s1 (str): 1st string
        s2 (str): 2nd string
        sort_names (bool, optional): Optional Parameter for sorting  string. Defaults to False.

    Returns:
        float | None: Returns the ratio of fuzzy matching if no error occurs. Else None.
    """
    # Sort names needed/
    if sort_names:
        s1 = " ".join(sorted(s1.split(" ")))
        s2 = " ".join(sorted(s1.split(" ")))

    # Return None if an error occurs
    try:
        return round(fuzz.ratio(s1, s2)/100,4)
    except ValueError:
        return None

def format_data(sdn_name: pd.Series) -> pd.Series:
    """
    Summary:
        Function for formatting a pandas Series object.

    Args:
        sdn_name (pd.Series): pandas Series object to be formatted.

    Returns:
        pd.Series: Returns the formatted pandas Series object.
    """
    # Replace "/" and "-" with a space
    sdn_name = sdn_name.str.replace("[^a-zA-Z0-9 ]", "", regex=True)

    # Convert the string to uppercase
    sdn_name = sdn_name.str.upper()

    # Replace all non alphanumeric with an empty string
    sdn_name = sdn_name.replace("[^a-zA-Z0-9 ]", "", regex=True)

    # Replace all the multispace with a single space
    sdn_name = sdn_name.replace("\s+", " ", regex=True)

    return sdn_name

@screen_app.get("/")
async def root(request: Request):
    return templates.TemplateResponse("screen.html",{"request":request, "response": {"data": []}})

@screen_app.get("/screen")
async def screen(name: str, threshold: float = 0.7):
    cleaned_name = cleanning_names(name)
    sanctions = get_consolidated_table()

    # Debugging Statements
    print("Name Var: ",name)
    print("Functions cleanning_names: ",cleanning_names(name))
    print("Cleaned_name Var:",cleaned_name)
    print("Fuzzy Matching",get_ratio(name, cleaned_name))

    # Ensure data is present
    if sanctions.empty:
        return {
            "status": "error",
            "message": "No data found in the ofac_consolidated table"
        }

    # Format sdn_name
    sanctions["sdn_name"] = format_data(sanctions["sdn_name"])

    # Calculate similarity scores using vectorized approach
    similarity_scores = np.vectorize(get_ratio)(sanctions["sdn_name"], cleaned_name)
    sanctions["similarity_score"] = similarity_scores

    # Filter based on similarity score (adjust threshold as needed)
    sanctions_filtered = sanctions[sanctions["similarity_score"] >= threshold]

    # Debugging Statements
    print(similarity_scores)
    print(sanctions["similarity_score"])

    # Handle potential empty response
    if sanctions_filtered.empty:
        return {
            "status": "info",
            "message": "No matches found based on the provided name and threshold."
        }

    # Prepare response dictionary
    response = sanctions_filtered.fillna("-").to_dict(orient="records")

    return response
