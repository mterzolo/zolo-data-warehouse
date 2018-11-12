import yaml
import logging
import warnings
import pandas as pd
from sqlalchemy import create_engine
import datetime as dt
import numpy as np
import pickle

from quickbooks import QuickBooks
from quickbooks import Oauth2SessionManager


# Ignore warnings
warnings.filterwarnings("ignore")

# Load config file
with open("../../config.yml", 'r') as infile:
    cfg = yaml.load(infile)

# Load session manager
with open('../../session_manager.pkl', 'rb') as file:
    session_manager = pickle.load(file)

# Get start and end dates
end_date = dt.datetime.utcnow().isoformat()
start_date = cfg['last_update_quickbooks']

# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler
handler = logging.FileHandler('../../logs/quickbooks/{}.log'.format(end_date))
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
    load(orders_dfs)

    # Update config file with last_update
    cfg['last_update_shopify'] = end_date
    with open('../../config.yml', 'w') as outfile:
        yaml.dump(cfg, outfile, default_flow_style=False)


def extract(start_date, end_date):
    """
    Connect to the shopify API and pull orders for the given time period
    :param start_date: timestamp indicating beginning of time range
    :param end_date: timestamp indicating end of time range
    :return: list of JSON responses
    """

    logger.info('Begin Extract')

    # Refresh token
    session_manager.refresh_access_tokens(cfg['quickbooks_refresh_token'])

    # Create client
    client = QuickBooks(
        sandbox=False,
        session_manager=session_manager,
        company_id=cfg['quickbooks_realm_id'],
        refresh_token=cfg['quickbooks_refresh_token'],
        minorversion=4
    )

    # Get total number of reports
    response = client.query("""
                            select count(*) from Invoice 
                            where TxnDate > '{}' and TxnDate < '{}'
                            """.format(start_date, end_date))
    num_invoices = response['QueryResponse']['totalCount']

    orders = []

    for page in range(round(num_invoices / 25)):
        response = client.query("""
                        select * from Invoice 
                        where TxnDate > '{}' and TxnDate < '{}'
                        STARTPOSITION {} MAXRESULTS {}
                        """.format(start_date,
                                   end_date,
                                   (page * 25 + 1),
                                   25))
        orders.append(response)

    logger.info('Data Extraction completed successfully')

    return orders


def transform(orders):
    """
    Takes the raw JSON responses and formats them into tables for the database
    :param orders: list of JSON resonses with order information
    :return: dataframes that will be the orders and order_details tables
    """

    logger.info('Begin data transformation')

    order_dfs = []

    for batch in orders:

        for response in batch['QueryResponse']['Invoice']:

            # Select all relevant data points
            payment_id = response['DocNumber']
            created_at = response['TxnDate']
            customer_id = response['CustomerRef']['value']

            # Filter out irrelevant dicts from line details
            new_lines = [i for i in response['Line'] if 'Id' in i.keys()]

            # Quickbooks id
            quickbooks_id = []
            for line in new_lines:
                try:
                    quickbooks_id.append(line['SalesItemLineDetail']['ItemRef']['value'])
                except KeyError:
                    quickbooks_id.append(np.nan)

            # Quantity
            quantity = []
            for line in new_lines:
                try:
                    quantity.append(line['SalesItemLineDetail']['Qty'])
                except KeyError:
                    quantity.append(np.nan)

            # Price
            price = []
            for line in new_lines:
                try:
                    price.append(line['SalesItemLineDetail']['UnitPrice'])
                except KeyError:
                    price.append(np.nan)

            temp_df = pd.DataFrame({'payment_id': payment_id,
                                    'created_at': created_at,
                                    'customer_id': customer_id,
                                    'quickbooks_id': quickbooks_id,
                                    'quantity': quantity,
                                    'price': price})
            order_dfs.append(temp_df)

    try:
        invoices = pd.concat(order_dfs)
    except ValueError:
        invoices = pd.DataFrame(columns=[
            'payment_id',
            'created_at',
            'customer_id',
            'quickbooks_id',
            'quantity',
            'price'
        ])

    # Calc total dollars for the invoice
    invoices['dollars'] = invoices['quantity'] * invoices['price']

    # Create aggregation instructions
    agg_dict = {
        'quantity':'sum',
        'price':'sum',
        'dollars':'sum'
    }

    # Agg lines items to get invoice summary
    qb_trans = invoices.groupby(['payment_id', 'created_at', 'customer_id']).agg(agg_dict).reset_index()

    qb_trans = qb_trans.loc[:, [

        'payment_id',
        'created_at',
        'customer_id',
        'dollars',
    ]]

    # Create the shopify transaction details table
    qb_trans_details = invoices.loc[:, [

        'payment_id',
        'quickbooks_id',
        'quantity',
        'price',
        'dollars'
    ]]

    logger.info('Data transformation completed successfully')

    return qb_trans_details, qb_trans


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
    orders_df[0].to_sql('qb_trans_details', con=engine, if_exists='append', index=False)
    orders_df[1].to_sql('qb_trans', con=engine, if_exists='append', index=False)

    logger.info('Loading {} records to square_trans_details'.format(len(orders_df[0])))
    logger.info('Loading {} records to square_trans'.format(len(orders_df[1])))
    logger.info('Data load completed successfully')


# Main section
if __name__ == '__main__':
    main()