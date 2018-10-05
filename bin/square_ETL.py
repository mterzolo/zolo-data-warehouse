import pandas as pd
import numpy as np
import datetime as dt
import re

from squareconnect.apis.v1_transactions_api import V1TransactionsApi
from squareconnect.rest import ApiException

from sqlalchemy import create_engine
import yaml

# Load config file
with open("../config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

# Get start and end dates
today = dt.datetime.today().strftime('%Y-%m-%d')
week_ago = dt.datetime.today() - dt.timedelta(7)
week_ago = week_ago.strftime('%Y-%m-%d')


def main():
    """
    Main entry point for the code
    :return:
    """

    payments = extract(week_ago, today)
    data_trans = transform(payments)
    load(data_trans)


def extract(start_date, end_date):
    """
    Pull data from the square API
    :return: JSON response with orders in the date_range provided
    """

    # Create an instance of the Location API class
    api_instance = V1TransactionsApi()

    # Setup authorization
    api_instance.api_client.configuration.access_token = cfg['square_token']

    # Helper variables
    payments = []
    has_next_page = True
    batch_token = None

    try:
        while has_next_page:

            # Send request
            api_response = api_instance.list_payments(location_id=cfg['square_location_id'],
                                                      batch_token=batch_token,
                                                      begin_time=start_date,
                                                      end_time=end_date)
            payments.append(api_response)

            try:

                # Get Batch Token
                response_link = api_instance.api_client.last_response.getheader('Link')
                batch_token = re.search(r'batch_token=(.*?)&begin_time=', response_link).group(1)
            except TypeError:
                has_next_page = None
                batch_token = None
    except ApiException as e:
        print('Exception when calling V1TransactionsApi->list_payments: %s\n' % e)

    return payments


def transform(payments):
    """
    Takes the response from the API and prepares it to be loaded into the data warehouse
    :return:
    """

    # Unpack array
    payments_dfs = []

    for batch in payments:

        for response in batch:
            # Convert to dict
            batch_dict = response.to_dict()

            # Select all relevant data points
            payment_id = batch_dict['id']
            created_at = batch_dict['created_at']
            device_name = batch_dict['device']['name']
            product_name = [i['name'] for i in batch_dict['itemizations']]
            quantity = [i['quantity'] for i in batch_dict['itemizations']]
            sku = [i['item_detail']['sku'] for i in batch_dict['itemizations']]
            category_name = [i['item_detail']['category_name'] for i in batch_dict['itemizations']]
            dollars = [int(i['total_money']['amount']) / 100 for i in batch_dict['itemizations']]

            # Create dataframe for the row(s)
            temp_df = pd.DataFrame({
                'payment_id': payment_id,
                'created_at': created_at,
                'device_name': device_name,
                'product_name': product_name,
                'quantity': quantity,
                'sku': sku,
                'category_name': category_name,
                'dollars': dollars
            })

            payments_dfs.append(temp_df)

    data = pd.concat(payments_dfs).reset_index(drop=True)

    # Clean up date field
    data['created_at'] = pd.to_datetime(data['created_at'])
    data['date_rounded'] = data['created_at'] - pd.to_timedelta(data['created_at'].dt.dayofweek, unit='d')
    data['date_rounded'] = data['date_rounded'].dt.date

    # Clean up names and filter irrelevant data
    data['name_clean'] = np.where(data['product_name'].isin(['Mamazolo Classic Espresso Roast',
                                                             "Mamazolo's Classic Espresso Roast"]),
                                  "Mamazolo's Classic Espresso Roast",
                                  'other')
    data['name_clean'] = np.where(data['product_name'].isin(['Ethiopia Guji Lot 003',
                                                             'Ethiopia Konga Kebele',
                                                             'Ethiopia Natural Gedeb',
                                                             'Ethiopian DP Yirgacheffe Aricha',
                                                             'Ethiopian DP Yirgacheffe Gedeo Worka']),
                                  'Ethiopian Banko Fuafuate',
                                  data['name_clean'])
    data['name_clean'] = np.where(data['product_name'].isin(['Vita Bella', 'Vita Bella Decaf']),
                                  'Vita Bella Decaf',
                                  data['name_clean'])
    data['name_clean'] = np.where(data['product_name'].isin(['Little Dog',
                                                             'Monkey See Monkey Do',
                                                             'Ocho Estrellas',
                                                             'Farmhouse French']),
                                  data['product_name'],
                                  data['name_clean'])

    # Map drinks to coffees
    coffee_map = {
        'Americano': "Mamazolo's Classic Espresso Roast",
        'Cafe au Lait': 'Farmhouse French',
        'Cappuccino': "Mamazolo's Classic Espresso Roast",
        'Cappuccino-F': "Mamazolo's Classic Espresso Roast",
        'Cappucino': "Mamazolo's Classic Espresso Roast",
        'Cold Brew Iced Coffee': 'Farmhouse French',
        'Espresso Shot': "Mamazolo's Classic Espresso Roast",
        'Espresso Shot-F': "Mamazolo's Classic Espresso Roast",
        'Ethiopia Natural Fuafuante': 'Ethiopian Banko Fuafuate',
        'Extra Shot Espresso': "Mamazolo's Classic Espresso Roast",
        'Fancy Latte': "Mamazolo's Classic Espresso Roast",
        'Farmhouse French': 'Farmhouse French',
        'Farmhouse French ICED': 'Farmhouse French',
        'Farmhouse French Iced Coffee': 'Farmhouse French',
        'Fast Farmhouse': 'Farmhouse French',
        'Iced Coffee Medium Roast': 'Little Dog',
        'Iced Espresso': "Mamazolo's Classic Espresso Roast",
        'Latte': "Mamazolo's Classic Espresso Roast",
        'Latte-F': "Mamazolo's Classic Espresso Roast",
        'Little Dog': 'Little Dog',
        'Macchiato': "Mamazolo's Classic Espresso Roast",
        'Mocha': "Mamazolo's Classic Espresso Roast",
        'Monkey See Monkey Do': 'Monkey See Monkey Do',
        'New Orleans Style Iced Coffee': 'Farmhouse French',
        'Nitro Cold Brew': 'Farmhouse French',
        'Ocho Estrellas': 'Ocho Estrellas',
        'Red Eye': "Mamazolo's Classic Espresso Roast",
        'Vita Bella Decaf': 'Vita Bella Decaf'
    }
    data['name_clean'] = data['product_name'].map(coffee_map)

    # Filter out irrelvant coffees
    data = data[data['name_clean'] != 'other']
    data = data[data['name_clean'].notnull()]

    # Impute missing decaf data for milk drinks
    data_decaf_milk = data[data['category_name'] == 'Milk Drinks']
    data_decaf_milk['quantity'] = .05
    data_decaf_milk['name_clean'] = 'Vita Bella Decaf'

    # Union back to original dataset
    data['quantity'] = np.where(data['category_name'] == 'Milk Drinks', .95, data['quantity'])
    data = pd.concat((data, data_decaf_milk), axis=0)

    # Calc weight (assume 30 grams per drink)
    data['weight'] = np.where(data['category_name'] == 'Roasted Coffee',
                              data['quantity'] * .75,
                              data['quantity'] * 0.0661387)
    data['type'] = np.where(data['category_name'] == 'Roasted Coffee', 'bags', 'loose')

    # Aggregate and save to disk
    data_trans = data.groupby(['date_rounded', 'name_clean', 'type']).sum()['weight'].reset_index()

    return data_trans


def load(data_trans):
    """
    Take the transformed data and load to database
    :param data_trans:
    :return:
    """

    # Create connection engine
    engine = create_engine('postgresql://{}:{}@{}/{}'.format(cfg['db_user_name'],
                                                             cfg['db_password'],
                                                             cfg['db_IP'],
                                                             cfg['db_name']))

    # Load to database
    data_trans.to_sql('square_transactions', con=engine, if_exists='append', index=False)


# Main section
if __name__ == '__main__':
    main()
