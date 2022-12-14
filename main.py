from utility import digit_cleaner, write_csv_file
import mysql.connector as connector
from datetime import datetime as dt
from bs4 import BeautifulSoup
import pandas as pd
import requests
import json
import csv
import os


url = os.environ.get('url')
headers = json.loads(os.environ.get('header'))

#Find elements
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, 'html.parser')

main_div = soup.find('div', id='nav-today')
table = main_div.find('table', id='main_table_countries_today')

column_names_raw = table.find_all('th')
column_names_clean = [c.text.strip() for c in column_names_raw][1:15]

row_data_raw = main_div.find('tbody').find_all('tr')
rows_data_raw = [[x for x in r.find_all('td')] for r in row_data_raw]
rows_data_clean = [[y.text.strip() for y in x if y.text.strip()][1:15] for x in rows_data_raw]
rows_data_sql = [list([None for _ in range(len(column_names_clean))]) for _ in range(len(rows_data_clean))]
rows_data_sql_done = []

CLEANED_DATA_SQL = []

for x, y in zip(rows_data_clean, rows_data_sql):
    for sx, sy in zip(x, y):
        ix = x.index(sx)
        y[ix] = x[ix]

for c in rows_data_sql:
    country, total_cases, \
    new_cases, total_deaths, \
    new_deaths, total_recovered, \
    new_recovered, active_cases, \
    serious_critical, top_cases_m1_pop, \
    deaths_m1_pop, total_test, test_m1_pop, \
    population = c
        
    country, total_cases = country, digit_cleaner(str(total_cases))
    new_cases, total_deaths = digit_cleaner(str(new_cases)), digit_cleaner(str(total_cases))
    new_deaths, total_recovered = digit_cleaner(str(new_deaths)), digit_cleaner(str(total_recovered))
    new_recovered, active_cases = digit_cleaner(str(new_recovered)), digit_cleaner(str(active_cases))
    serious_critical, top_cases_m1_pop = digit_cleaner(str(serious_critical)), digit_cleaner(str(top_cases_m1_pop))
    deaths_m1_pop, total_test, test_m1_pop = digit_cleaner(str(deaths_m1_pop)), digit_cleaner(str(total_test)), digit_cleaner(str(test_m1_pop))
    population = digit_cleaner(str(population))

    CLEANED_DATA_SQL.append((
                        country.lower(), total_cases, new_cases,
                        total_deaths if total_deaths else None, new_deaths if new_deaths else None, total_recovered if total_recovered else None,
                        new_recovered if new_recovered else None, active_cases if active_cases else None, serious_critical if serious_critical else None,
                        top_cases_m1_pop if top_cases_m1_pop else None, deaths_m1_pop if deaths_m1_pop else None, total_test if total_test else None,
                        test_m1_pop if test_m1_pop else None, population if population else None, dt.now(), dt.now().time()
                        ))

with connector.connect(host=os.environ.get('host'), user=os.environ.get('user'), password=os.environ.get('password'), database=os.environ.get('database')) as connection:
    cursor = connection.cursor()
    
    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS
                    covid_stats(
                                id INT PRIMARY KEY AUTO_INCREMENT,
                                country VARCHAR(80),
                                total_cases VARCHAR(80),
                                new_cases VARCHAR(80),
                                total_deaths VARCHAR(80),
                                new_deaths VARCHAR(80),
                                total_recovered VARCHAR(80),
                                new_recovered VARCHAR(80),
                                active_cases VARCHAR(80),
                                serious_critical VARCHAR(80),
                                tot_cases_m1_pop VARCHAR(80),
                                deaths_m1_pop VARCHAR(80),
                                total_tests VARCHAR(80),
                                tests_1m_pop VARCHAR(80),
                                population VARCHAR(80),
                                date DATE,
                                time TIME
                               )
                   """)

    sql = "INSERT INTO covid_stats VALUES(NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    for c in CLEANED_DATA_SQL[8:]:
        cursor.execute(sql, c)

    connection.commit()


    search_country = input("")

write_csv_file('covid_stats', row=column_names_clean, rows=rows_data_clean)
