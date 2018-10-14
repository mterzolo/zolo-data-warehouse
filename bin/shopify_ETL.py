import yaml
import logging
import warnings
import pandas as pd
from sqlalchemy import create_engine

import requests
import datetime as dt
import numpy as np
import math


# Ignore warnings
warnings.filterwarnings("ignore")

# Load config file
with open("../config.yml", 'r') as infile:
    cfg = yaml.load(infile)

# Get start and end dates
end_date = dt.datetime.utcnow().isoformat()
start_date = cfg['last_update_shopify']

# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler
handler = logging.FileHandler('../logs/shopify_{}.log'.format(end_date))
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)


def main():
    """
    Main entry point for the code
    :return:
    """

    logger.info('date_range for this ETL (UTC): {} - {}'.format(start_date, end_date))

    # Run ETL
    orders = extract(start_date, end_date)
    orders_dfs = transform(orders)
    #load(orders_dfs)

    # Update config file with last_update
    cfg['last_update_shopify'] = end_date
    with open('../config.yml', 'w') as outfile:
        yaml.dump(cfg, outfile, default_flow_style=False)


def extract(start_date, end_date):
    """
    Connect to the shopify API and pull orders for the given time period
    :param start_date: timestamp indicating beginning of time range
    :param end_date: timestamp indicating end of time range
    :return: list of JSON responses
    """

    logger.info('Begin Extract')

    # Create request strings
    shop_url = "https://{}:{}@{}.myshopify.com/admin".format(cfg['shopify_key'],
                                                             cfg['shopify_password'],
                                                             cfg['shopify_store_name'])
    query_count = '/orders/count.json?created_at_min={}?created_at_max{}'.format(start_date, end_date)

    # Make the request for counts
    r = requests.get(shop_url + query_count)
    orders_count = r.json()['count']

    orders = []

    # Loop through the pages and store the responses
    for page in range(math.ceil(orders_count / 50)):
        query_orders = shop_url + '/orders.json?created_at_min={}?created_at_max={}?limit=50&page={}'.format(start_date,
                                                                                                             end_date,
                                                                                                             page + 1)
        r = requests.get(query_orders)
        orders.append(r.json())

    logger.info('Data Extraction completed successfully')

    return orders


def transform(orders):
    """
    Takes the raw JSON responses and formats them into tables for the database
    :param orders: list of JSON resonses with order information
    :return: dataframes that will be the orders and order_details tables
    """

    logger.info('Begin data transformation')

    # Unpack response
    orders_dfs = []

    for batch in orders:

        for order in batch['orders']:

            # Pull out relevant data points
            order_id = order['id']
            created_at = order['created_at']
            product_name = [i['title'] for i in order['line_items']]
            quantity = [int(i['quantity']) for i in order['line_items']]
            sku = [i['sku'] for i in order['line_items']]
            price = [float(i['price']) for i in order['line_items']]
            try:
                shipping_price = [float(i['price']) for i in order['shipping_lines']][0]
            except IndexError:
                shipping_price = np.nan

            # Store in dataframe
            temp_df = pd.DataFrame({
                'order_id': order_id,
                'created_at': created_at,
                'product_name': product_name,
                'quantity': quantity,
                'sku': sku,
                'price': price,
                'shipping_price': shipping_price
            })
            orders_dfs.append(temp_df)

    # Union all dataframes
    try:
        data = pd.concat(orders_dfs)
    except ValueError:
        data = pd.DataFrame(columns=[

            'order_id',
            'created_at',
            'product_name',
            'quantity',
            'sku',
            'price',
            'shipping_price'
        ])

    # Transform to datetime object
    data['created_at'] = pd.to_datetime(data['created_at'])
    data['created_at'] = data['created_at'] - dt.timedelta(hours=7)

    # Calc the total dollars for the line item
    data['subtotal'] = data['quantity'] * data['price']

    print(data['sku'].value_counts())

    # Create the shopify transactions table
    agg_dict = {
        'shipping_price': 'min',
        'subtotal': 'sum',
    }

    shopify_trans = data.groupby(['order_id', 'created_at', 'sku']).agg(agg_dict).reset_index()
    shopify_trans['total_dollars'] = shopify_trans['subtotal'] + shopify_trans['shipping_price']

    shopify_trans = shopify_trans.loc[:, [

        'order_id',
        'created_at',
        'shipping_price',
        'subtotal',
        'total_dollars'
    ]]

    # Create the shopify transaction details table
    shopify_details = data.loc[:, [

        'order_id',
        'sku',
        'quantity',
        'price'
    ]]

    logger.info('Data transformation completed successfully')

    return shopify_details, shopify_trans


def load(orders_df):
    """
    Load tables to the database
    :param orders_df: tuple of dataframes
    :return:
    """

    logger.info('Begin data load')

    # Create connection engine
    engine = create_engine('postgresql://{}:{}@{}/{}'.format(cfg['db_user_name'],
                                                             cfg['db_password'],
                                                             cfg['db_IP'],
                                                             cfg['db_name']))

    # Load to database
    orders_df[0].to_sql('shopify_trans_details', con=engine, if_exists='append', index=False)
    orders_df[1].to_sql('shopify_trans', con=engine, if_exists='append', index=False)

    logger.info('Data load completed successfully')


# Main section
if __name__ == '__main__':
    main()