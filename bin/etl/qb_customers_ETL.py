import yaml
import logging
import warnings
import pandas as pd
from sqlalchemy import create_engine
import datetime as dt
import psycopg2

from quickbooks import QuickBooks
from quickbooks import Oauth2SessionManager


# Ignore warnings
warnings.filterwarnings("ignore")

# Load config file
with open("../../config.yml", 'r') as infile:
    cfg = yaml.load(infile)

# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Get datetime
today = dt.datetime.utcnow().isoformat()

# Create a file handler
handler = logging.FileHandler('../../logs/qb_customers/{}.log'.format(today))
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

    logger.info('date_run for this ETL (UTC): {}'.format(today))

    # Run ETL
    customers = extract()
    customer_table = transform(customers)
    load(customer_table)

    # Update config file with last_update
    cfg['last_update_qb_customers'] = today
    with open('../../config.yml', 'w') as outfile:
        yaml.dump(cfg, outfile, default_flow_style=False)


def extract():
    """
    Get all customers on file
    :return: list of JSON responses
    """

    logger.info('Begin Extract')

    # Create session
    session_manager = Oauth2SessionManager(
        client_id=cfg['quickbooks_client_id'],
        client_secret=cfg['quickbooks_client_secret'],
        access_token=cfg['quickbooks_access_token'],
        base_url='https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl'
    )

    # Refresh token and save to config
    refresh = session_manager.refresh_access_tokens(cfg['quickbooks_refresh_token'], return_result=True)
    cfg['quickbooks_access_token'] = refresh['access_token']
    cfg['quickbooks_refresh_token'] = refresh['refresh_token']

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
                            select count(*) from Customers 
                            """)
    num_customers = response['QueryResponse']['totalCount']

    customers = []

    for page in range(round(num_customers / 25)):
        response = client.query("""
                        select * from Customers 
                        STARTPOSITION {} MAXRESULTS {}
                        """.format((page * 25 + 1), 25))
        customers.append(response)

    logger.info('Data Extraction completed successfully')

    return customers


def transform(customers):
    """
    Takes the raw JSON responses and formats them into tables for the database
    :param customers: list of JSON responses with customer information
    :return: dataframes that will be the orders and order_details tables
    """

    logger.info('Begin data transformation')

    customers_dfs = []

    for batch in customers:

        for response in batch['QueryResponse']['Invoice']:

            # Select all relevant data points
            customer_id = response['Id']
            customer_name = response['CompanyName']
            phone_number = response['PrimaryPhone']['FreeFormNumber']
            address = response['ShipAddr']['Line1']
            city = response['ShipAddr']['City']
            state = response['ShipAddr']['CountrySubDivisionCode']
            zipcode = response['ShipAddr']['PostalCode']
            create_date = response['MetaData']['CreateTime']

            temp_df = pd.DataFrame({'customer_id': customer_id,
                                    'customer_name': customer_name,
                                    'phone_number': phone_number,
                                    'address': address,
                                    'city': city,
                                    'state': state,
                                    'zipcode': zipcode,
                                    'create_date': create_date})
            customers_dfs.append(temp_df)

    customers_table = pd.concat(customers_dfs)

    logger.info('Data transformation completed successfully')

    return customers_table


def load(customers_table):
    """
    Load tables to the database
    :param customers_table: tuple of dataframes
    :return:
    """

    logger.info('Begin data load')

    # Connect to database
    conn = psycopg2.connect("host={} dbname={} user={} password={}".format(cfg['db_IP'],
                                                                           cfg['db_name'],
                                                                           cfg['db_user_name'],
                                                                           cfg['db_password']))

    # Create table
    cur = conn.cursor()
    cur.execute("""
    DROP TABLE IF EXISTS qb_customers

    CREATE TABLE qb_customers(
        customer_id text,
        customer_name text,
        phone_number text,
        address text,
        city text,
        state text,
        zipcode text,
        create_date timestamp,
        
    );
    """)

    # Create connection engine
    engine = create_engine('postgresql://{}:{}@{}/{}'.format(cfg['db_user_name'],
                                                             cfg['db_password'],
                                                             cfg['db_IP'],
                                                             cfg['db_name']))

    # Load to database
    customers_table.to_sql('qb_customers', con=engine, if_exists='replace', index=False)

    logger.info('Loading {} records to qb_customers'.format(len(customers_table)))
    logger.info('Data load completed successfully')


# Main section
if __name__ == '__main__':
    main()