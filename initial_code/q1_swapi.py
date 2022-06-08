import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Integer, String
import psycopg2 
import io
import json
engine = create_engine('postgresql://postgres:admin@localhost:5432/postgres')
# Create a metadata instance
metadata = MetaData(engine)

film_json = requests.get("https://swapi.dev/api/films/").json()['results']
film_df = pd.json_normalize(film_json, max_level=1)
film_df.head()

film_df.info()

people_json = requests.get("https://swapi.dev/api/people/").json()['results']
people_df = pd.json_normalize(people_json, max_level=1)
people_df.info()

planets_json = requests.get("https://swapi.dev/api/planets/").json()['results']
# print(planets_json)
# type(planets_json)

planets_df = pd.DataFrame()
for planet in planets_json:
    print(planet['url'])
    df = pd.DataFrame(columns=['url', 'string_content'])
    df = df.append({'url': planet['url'], 'string_content': planet}, ignore_index=True)
    planets_df = pd.concat([planets_df, df])

planets_df.info()