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
DROP TABLE IF EXISTS square_trans;

CREATE TABLE square_trans(
    payment_id text,
    created_at timestamp,
    market text,
    dollars float,
    tendered_cash float,
    returned_cash float
);

DROP TABLE IF EXISTS square_trans_details;

CREATE TABLE square_trans_details(
    payment_id text,
    square_id text,
    quantity int,
    dollars float,
    modifiers text,
    variation_name text
);
""")
conn.commit()