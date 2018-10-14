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

# Get start and end dates
end_date = dt.datetime.utcnow().isoformat()
start_date = cfg['last_update_square']

# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler
handler = logging.FileHandler('../logs/square_{}.log'.format(end_date))
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
    payments = extract(start_date, end_date)
    trans_dfs = transform(payments)
    load(trans_dfs)

    # Update config file with last_update
    cfg['last_update_square'] = end_date
    with open('../config.yml', 'w') as outfile:
        yaml.dump(cfg, outfile, default_flow_style=False)


def extract(start_date, end_date):
    """
    Pull data from the square API
    :return: JSON response with orders in the date_range provided
    """

    logger.info('Begin Extract')

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
        logger.debug('Exception when calling V1TransactionsApi->list_payments: %s\n' % e)

    logger.info('Data Extraction completed successfully')

    return payments


def transform(payments):
    """
    Takes the response from the API and prepares it to be loaded into the data warehouse
    :return:
    """

    logger.info('Begin data transformation')

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
            quantity = [i['quantity'] for i in batch_dict['itemizations']]
            sku = [i['item_detail']['sku'] for i in batch_dict['itemizations']]
            dollars = [int(i['total_money']['amount']) / 100 for i in batch_dict['itemizations']]
            variation_name =[i['item_variation_name'] for i in batch_dict['itemizations']]

            try:
                tendered_cash = int(batch_dict['tender'][0]['tendered_money']['amount']) / 100
                returned_cash = int(batch_dict['tender'][0]['change_back_money']['amount']) / 100
            except TypeError:
                tendered_cash = np.nan
                returned_cash = np.nan
            try:
                modifiers = [';'.join(j['name'] for j in i['modifiers']) for i in batch_dict['itemizations']]
            except TypeError:
                modifiers = np.nan

            # Create dataframe for the row(s)
            temp_df = pd.DataFrame({
                'payment_id': payment_id,
                'created_at': created_at,
                'device_name': device_name,
                'quantity': quantity,
                'sku': sku,
                'dollars': dollars,
                'tendered_cash': tendered_cash,
                'returned_cash': returned_cash,
                'modifiers': modifiers,
                'variation_name': variation_name
            })

            payments_dfs.append(temp_df)

    try:
        data = pd.concat(payments_dfs).reset_index(drop=True)
    except ValueError:
        data = pd.DataFrame(columns=[
            'payment_id',
            'created_at',
            'device_name',
            'quantity',
            'sku',
            'dollars',
            'tendered_cash',
            'returned_cash',
            'modifiers',
            'variation_name'
        ])

    # Clean up date field
    data['created_at'] = pd.to_datetime(data['created_at'])
    data['created_at'] = data['created_at'] - dt.timedelta(hours=7)
    data['date'] = data['created_at'].dt.date
    data['time'] = data['created_at'].dt.time

    # Get day of week and first transaction of the day
    data['DOW'] = data['created_at'].dt.dayofweek
    data['first_trans'] = data.groupby(['date', 'device_name'])['time'].transform('min')

    # Determine market
    data['market'] = np.where(data['DOW'] == 3, 'San Rafael Thurs', 'other')
    data['market'] = np.where(data['DOW'] == 5, 'Danville Farmers Market', data['market'])
    data['market'] = np.where((data['DOW'] == 6) &
                              (data['first_trans'] < dt.time(7)), 'Alameda Antique Faire', data['market'])
    data['market'] = np.where((data['DOW'] == 6) &
                              (data['first_trans'] > dt.time(7)), 'San Rafael Sunday', data['market'])

    # Create transactions details table
    data_trans_details = data.loc[:, [
       'payment_id',
       'sku',
       'quantity',
       'dollars',
       'modifiers',
       'variation_name'

       ]]

    # Create transactions table
    agg_dict = {
        'dollars':'sum',
        'tendered_cash':'min',
        'returned_cash':'min',
    }
    data_trans = data.groupby(['payment_id', 'created_at', 'market']).agg(agg_dict).reset_index()

    logger.info('Data transformation completed successfully')

    return data_trans_details, data_trans


def load(trans_dfs):
    """
    Take the transformed data and load to database
    :param trans_dfs: tuple of dataframes
    :return:
    """

    logger.info('Begin data load')

    # Create connection engine
    engine = create_engine('postgresql://{}:{}@{}/{}'.format(cfg['db_user_name'],
                                                             cfg['db_password'],
                                                             cfg['db_IP'],
                                                             cfg['db_name']))

    # Load to database
    trans_dfs[0].to_sql('square_trans_details', con=engine, if_exists='append', index=False)
    trans_dfs[1].to_sql('square_trans', con=engine, if_exists='append', index=False)

    logger.info('Data load completed successfully')


# Main section
if __name__ == '__main__':
    main()
