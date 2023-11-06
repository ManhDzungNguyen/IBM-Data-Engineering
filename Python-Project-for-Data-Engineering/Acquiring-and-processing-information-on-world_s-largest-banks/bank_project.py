# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
db_name = "Banks.db"
table_name = "Largest_banks"
csv_path = "./Largest_banks_data.csv"


def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)

    page = requests.get(url).text
    data = BeautifulSoup(page, "html.parser")
    tables = data.find_all("tbody")
    rows = tables[0].find_all("tr")

    for row in rows:
        col = row.find_all("td")
        if len(col) != 0:
            data_dict = {
                "Name": col[1].get_text(strip=True),
                "MC_USD_Billion": float(col[2].get_text(strip=True)),
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True, join="inner")

    return df


def transform(df):
    exchange_rate = pd.read_csv("exchange_rate.csv")
    exchange_rate = exchange_rate.set_index("Currency")["Rate"].to_dict()

    for currency in exchange_rate:
        df[f"MC_{currency}_Billion"] = df["MC_USD_Billion"].apply(
            lambda x: np.round(x * exchange_rate[f"{currency}"], 2)
        )

    return df


def load_to_csv(df, csv_path):
    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


def log_progress(message):
    timestamp_format = r"%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + " : " + message + "\n")


if __name__ == "__main__":
    # log_progress('Preliminaries complete. Initiating ETL process')

    df = extract(url, table_attribs)

    log_progress("Data extraction complete. Initiating Transformation process")

    df = transform(df)

    log_progress("Data transformation complete. Initiating loading process")

    load_to_csv(df, csv_path)

    log_progress("Data saved to CSV file")

    sql_connection = sqlite3.connect(db_name)

    log_progress("SQL Connection initiated.")

    load_to_db(df, sql_connection, table_name)

    log_progress("Data loaded to Database as table. Running the query")

    query_statement = f"SELECT * FROM Largest_banks"
    run_query(query_statement, sql_connection)

    query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
    run_query(query_statement, sql_connection)

    query_statement = f"SELECT Name from Largest_banks LIMIT 5"
    run_query(query_statement, sql_connection)

    log_progress("Process Complete.")

    sql_connection.close()
