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
DROP TABLE IF EXISTS qb_trans;

CREATE TABLE qb_trans(
    payment_id text,
    created_at timestamp,
    dollars float,
    customer_id text
);

DROP TABLE IF EXISTS qb_trans_details;

CREATE TABLE qb_trans_details(
    payment_id text,
    quickbooks_id text,
    quantity int,
    price float,
    dollars float
);
""")
conn.commit()