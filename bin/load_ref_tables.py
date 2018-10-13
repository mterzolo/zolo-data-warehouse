import psycopg2
import yaml
import pandas as pd
from sqlalchemy import create_engine

# Load config file
with open("../config.yml", 'r') as infile:
    cfg = yaml.load(infile)

# Connect to database
conn = psycopg2.connect("host={} dbname={} user={} password={}".format(cfg['db_IP'],
                                                                       cfg['db_name'],
                                                                       cfg['db_user_name'],
                                                                       cfg['db_password']))

# Create table
cur = conn.cursor()
cur.execute("""

    DROP TABLE IF EXISTS square_items;
    
    CREATE TABLE square_items(
        square_sku text,
        item_name text,
        category_name text,
        profile_id int,
        weight float,
        form text
        );
        
    DROP TABLE IF EXISTS coffee_profiles;
    
    CREATE TABLE coffee_profiles(
        profile_id int,
        profile_name text,
        roast_level text,
        single_origin int,
        c1_origin text,
        c1_process text,
        c1_percent float,
        c2_origin text,
        c2_process text,
        c2_percent float,
        c3_procss text,
        c3_origin text,
        c3_percent float
        );
    
""")
conn.commit()

# Load data
square_items = pd.read_csv('../data/ref_tables - square_items.csv')
coffee_profiles = pd.read_csv('../data/ref_tables - coffee_profiles.csv')

# Create connection engine
engine = create_engine('postgresql://{}:{}@{}/{}'.format(cfg['db_user_name'],
                                                         cfg['db_password'],
                                                         cfg['db_IP'],
                                                         cfg['db_name']))

# Load to database
square_items.to_sql('square_items', con=engine, if_exists='replace', index=False)
coffee_profiles.to_sql('coffee_profiles', con=engine, if_exists='replace', index=False)