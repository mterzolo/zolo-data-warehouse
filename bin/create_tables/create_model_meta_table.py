import psycopg2
import yaml

# Load config file
with open("../../config.yml", 'r') as infile:
    cfg = yaml.load(infile)

# Connect to database
conn = psycopg2.connect("host={} dbname={} user={} password={}".format(cfg['db_IP'],
                                                                       cfg['db_name'],
                                                                       cfg['db_user_name'],
                                                                       cfg['db_password']))

# Create table
cur = conn.cursor()
cur.execute("""
DROP TABLE IF EXISTS model_meta;

CREATE TABLE model_meta(

    profile_name text,
    form text,
    best_config text,
    mse float,
    prediction float,
    std_error float,
    lower_bound float,
    upper_bound float,
    forecast_start timestamp
);
""")
conn.commit()