import re
import yaml
import logging
import warnings
import pandas as pd
import numpy as np
import datetime as dt
from squareconnect.apis.v1_transactions_api import V1TransactionsApi
from squareconnect.rest import ApiException
from sqlalchemy import create_engine

# Ignore warnings
warnings.filterwarnings("ignore")

# Load config file
with open("../config.yml", 'r') as infile:
    cfg = yaml.load(infile)


def main():
    """
    Main entry point for the code
    :return:
    """

    logging.getLogger().setLevel(level=logging.INFO)

    # Get start and end dates
    end_date = dt.datetime.today().isoformat()
    start_date = cfg['last_update']

    logging.info('date_range for this ETL: {} - {}'.format(start_date, end_date))

    # Run ETL
    payments = extract(start_date, end_date)
    trans_dfs = transform(payments)
    load(trans_dfs)

    # Update config file with last_update
    cfg['last_update'] = end_date
    with open('../config.yml', 'w') as outfile:
        yaml.dump(cfg, outfile, default_flow_style=False)


def extract(start_date, end_date):
    """
    Pull data from the square API
    :return: JSON response with orders in the date_range provided
    """

    logging.info('Begin Extract')

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

    logging.info('Data Extraction completed successfully')

    return payments


def transform(payments):
    """
    Takes the response from the API and prepares it to be loaded into the data warehouse
    :return:
    """

    logging.info('Begin data transformation')

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
            try:
                tendered_cash = int(batch_dict['tender'][0]['tendered_money']['amount']) / 100
                returned_cash = int(batch_dict['tender'][0]['change_back_money']['amount']) / 100
            except TypeError:
                tendered_cash = np.nan
                returned_cash = np.nan

            # Create dataframe for the row(s)
            temp_df = pd.DataFrame({
                'payment_id': payment_id,
                'created_at': created_at,
                'device_name': device_name,
                'product_name': product_name,
                'quantity': quantity,
                'sku': sku,
                'category_name': category_name,
                'dollars': dollars,
                'tendered_cash': tendered_cash,
                'returned_cash': returned_cash
            })

            payments_dfs.append(temp_df)

    try:
        data = pd.concat(payments_dfs).reset_index(drop=True)
    except ValueError:
        data = pd.DataFrame(columns=[
            'payment_id',
            'created_at',
            'device_name',
            'product_name',
            'quantity',
            'sku',
            'category_name',
            'dollars',
            'tendered_cash',
            'returned_cash'
        ])

    # Clean up date field
    data['created_at'] = pd.to_datetime(data['created_at'])
    data['date_rounded'] = data['created_at'] - pd.to_timedelta(data['created_at'].dt.dayofweek, unit='d')
    data['date_rounded'] = data['date_rounded'].dt.date
    data['time'] = data['created_at'].dt.time

    # Get day of week and first transaction of the day
    data['DOW'] = data['created_at'].dt.dayofweek
    data['first_trans'] = data.groupby(['date_rounded', 'device_name'])['time'].transform('min')

    # Determine Market
    data['market'] = np.where(data['DOW'] == 3, 'San Rafael Thurs', 'other')
    data['market'] = np.where(data['DOW'] == 5, 'Danville Farmers Market', data['market'])
    data['market'] = np.where((data['DOW'] == 6) &
                              (data['first_trans'] < dt.time(7)), 'Alameda Antique Faire', data['market'])
    data['market'] = np.where((data['DOW'] == 6) &
                              (data['first_trans'] > dt.time(7)), 'San Rafael Sunday', data['market'])

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
        'Avocado, Tomatoes, Cheddar & Eggs': 'food',
        'Cafe au Lait': 'Farmhouse French',
        'Cappuccino': "Mamazolo's Classic Espresso Roast",
        'Cappuccino-F': "Mamazolo's Classic Espresso Roast",
        'Cappucino': "Mamazolo's Classic Espresso Roast",
        'Cold Brew Iced Coffee': 'Farmhouse French',
        'Espresso Shot': "Mamazolo's Classic Espresso Roast",
        'Espresso Shot-F': "Mamazolo's Classic Espresso Roast",
        'Ethiopia Natural Fuafuante': 'Ethiopian Banko Fuafuate',
        'Ethiopia Guji Lot 003': 'Ethiopia Guji Lot 003',
        'Ethiopia Konga Kebele': 'Ethiopia Konga Kebele',
        'Ethiopia Natural Gedeb': 'Ethiopia Natural Gedeb',
        'Ethiopian DP Yirgacheffe Aricha': 'Ethiopian DP Yirgacheffe Aricha',
        'Ethiopian DP Yirgacheffe Gedeo Worka': 'Ethiopian DP Yirgacheffe Gedeo Worka',
        'Extra Shot Espresso': "Mamazolo's Classic Espresso Roast",
        'Fancy Latte': "Mamazolo's Classic Espresso Roast",
        'Farmhouse French': 'Farmhouse French',
        'Farmhouse French ICED': 'Farmhouse French',
        'Farmhouse French Iced Coffee': 'Farmhouse French',
        'Fast Farmhouse': 'Farmhouse French',
        'Hot Chocolate': 'Hot Chocolate',
        'Iced Coffee Medium Roast': 'Little Dog',
        'Iced/Hot Tea': 'Iced/Hot Tea',
        'Iced Espresso': "Mamazolo's Classic Espresso Roast",
        'Latte': "Mamazolo's Classic Espresso Roast",
        'Latte-F': "Mamazolo's Classic Espresso Roast",
        'LARGE SIZE': 'LARGE SIZE',
        'Little Dog': 'Little Dog',
        "Mamazolo's Classic Espresso Roast": "Mamazolo's Classic Espresso Roast",
        'Macchiato': "Mamazolo's Classic Espresso Roast",
        'Mocha': "Mamazolo's Classic Espresso Roast",
        'Monkey See Monkey Do': 'Monkey See Monkey Do',
        'New Orleans Style Iced Coffee': 'Farmhouse French',
        'Nitro Cold Brew': 'Farmhouse French',
        'Ocho Estrellas': 'Ocho Estrellas',
        'Porchetta & Eggs': 'food',
        'Red Eye': "Mamazolo's Classic Espresso Roast",
        'Refill Drip': 'Refill Drip',
        'Seasonal Toast': 'food',
        'Steamer': 'Steamer',
        'Sweet Toast': 'food',
        'Vanilla/Almond/Soy Add': 'Vanilla/Almond/Soy Add',
        'Vita Bella Decaf': 'Vita Bella Decaf',
        'Vita Bella': 'Vita Bella Decaf'
    }
    data['name_clean'] = data['product_name'].map(coffee_map)

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
    data['form'] = np.where(data['category_name'] == 'Roasted Coffee', 'bags', 'loose')

    # Create transactions details table
    data_trans_details = data.loc[:, [
       'payment_id',
       'name_clean',
       'category_name',
       'form',
       'quantity',
       'dollars',
       'weight',
       ]]

    # Create transactions table
    agg_dict = {
        'dollars':'sum',
        'tendered_cash':'min',
        'returned_cash':'min',
    }
    data_trans = data.groupby(['payment_id', 'created_at', 'market']).agg(agg_dict).reset_index()

    logging.info('Data transformation completed successfully')

    return data_trans_details, data_trans


def load(trans_dfs):
    """
    Take the transformed data and load to database
    :param trans_dfs: tuple of dataframes
    :return:
    """

    logging.info('Begin data load')

    # Create connection engine
    engine = create_engine('postgresql://{}:{}@{}/{}'.format(cfg['db_user_name'],
                                                             cfg['db_password'],
                                                             cfg['db_IP'],
                                                             cfg['db_name']))

    # Load to database
    trans_dfs[0].to_sql('square_trans_details', con=engine, if_exists='append', index=False)
    trans_dfs[1].to_sql('square_trans', con=engine, if_exists='append', index=False)

    logging.info('Data load completed successfully')


# Main section
if __name__ == '__main__':
    main()
