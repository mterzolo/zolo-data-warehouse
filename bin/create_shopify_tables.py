import psycopg2
import yaml

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
DROP TABLE IF EXISTS shopify_trans;

CREATE TABLE shopify_trans(
    order_id text,
    created_at timestamp,
    shipping_price float,
    total_price float

);

DROP TABLE IF EXISTS shopify_trans_details;

CREATE TABLE shopify_trans_details(
    order_id text,
    sku text,
    quantity int,
    price float

);
""")
conn.commit()
