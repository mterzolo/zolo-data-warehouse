import lib
import yaml
import logging
import warnings
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine


# Ignore warnings
warnings.filterwarnings("ignore")

# Load config file
with open("../../config.yml", 'r') as infile:
    cfg = yaml.load(infile)

# Store date information
today = dt.datetime.today()
forecast_start = today - dt.timedelta(days=today.weekday())
forecast_start = forecast_start.date()

# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a file handler
handler = logging.FileHandler('../../logs/models/arima_{}.log'.format(forecast_start))
handler.setLevel(logging.INFO)

# Create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(handler)


def main():
    """
    Main entry point for the code
    :return:
    """

    logger.info('start_date for this forecast: {}'.format(forecast_start))

    # Set model orders to search
    p_values = range(0, 5)
    d_values = range(0, 2)
    q_values = range(0, 5)

    # Run modelling script
    data = extract()
    model_data = transform(data)
    meta_df = model(model_data, p_values, d_values, q_values)
    load(meta_df)

    # Update config file with last_update
    cfg['last_model_run'] = forecast_start
    with open('../../config.yml', 'w') as outfile:
        yaml.dump(cfg, outfile, default_flow_style=False)


def extract():
    """
    Pull data from data base
    :return: pandas dataframe with historical sales data
    """

    logger.info('Begin Extract')

    engine = create_engine('postgresql://{}:{}@{}/{}'.format(cfg['db_user_name'],
                                                             cfg['db_password'],
                                                             cfg['db_IP'],
                                                             cfg['db_name']))
    query = """

    with square_weekly as (
    select
        p.profile_name,
        date_trunc('week', sqt.created_at) as week_date, 
        (i.weight * sqdt.quantity) as total_weight
    from square_trans as sqt
    left join square_trans_details as sqdt
    on sqt.payment_id = sqdt.payment_id
    left join items as i
    on sqdt.square_id = i.square_id
    inner join coffee_profiles as p
    on i.profile_id = p.profile_id
    where p.active = 1 and 
    created_at > '2017-10-1'),

    shopify_weekly as (
    select 
        p.profile_name,
        date_trunc('week', sht.created_at) as week_date, 
        (i.weight * shdt.quantity) as total_weight
    from shopify_trans as sht
    left join shopify_trans_details as shdt
    on sht.order_id = shdt.order_id
    left join items as i
    on shdt.shopify_id = cast(i.shopify_id as text)
    inner join coffee_profiles as p
    on i.profile_id = p.profile_id
    where p.active = 1 and 
    created_at > '2017-10-1'),
    
    quickbooks_weekly as (
    select 
        p.profile_name,
        date_trunc('week', qbt.created_at) as week_date, 
        (i.weight * qbdt.quantity) as total_weight
    from qb_trans as qbt
    left join qb_trans_details as qbdt
    on qbt.payment_id = qbdt.payment_id
    left join items as i
    on qbdt.quickbooks_id = cast(i.quickbooks_id as text)
    inner join coffee_profiles as p
    on i.profile_id = p.profile_id
    where p.active = 1 and 
    created_at > '2017-10-1')

    select
        u2.profile_name,
        u2.week_date, 
        sum(u2.total_weight) as weight
    from
    (select * 
    from
    (select *
    from shopify_weekly
    union all
    select * 
    from square_weekly) as u1
    union all 
    select * 
    from quickbooks_weekly
    ) as u2
    group by u2.profile_name, u2.week_date
    order by u2.profile_name, u2.week_date

    """.format(str(forecast_start), str(forecast_start))
    data = pd.read_sql_query(query, con=engine)

    logger.info('Data Extraction completed successfully')

    return data


def transform(data):
    """
    Takes the query and prepares the data for modelling
    :return:
    """

    logger.info('Begin data transformation')

    # Get relevant temporally relevant data
    data = data[data['week_date'] < forecast_start]

    # Exclude profile/forms with low counts
    data['week_count'] = data['week_date'].groupby(data['profile_name']).transform('count')
    data = data[data['week_count'] > 5]

    # Aggregate data
    model_data = data.groupby(['week_date', 'profile_name']).sum().reset_index()

    logger.info('Data transformation completed successfully')

    return model_data


def model(model_data, p_values, d_values, q_values):
    """
    Creates and evaluates models for all products
    :param model_data: Data that has been prepared for modelling
    :param p_values: list with all values of p to test
    :param d_values: list with all values of d to test
    :param q_values: list with all values of q to test
    :return: meta_df (dataframe containing model diagnostics and predictions)
    """

    logger.info('Begin modelling')

    # Create framework for final dataframe
    meta_df = pd.DataFrame(columns=[
        'profile_name',
        'best_config',
        'mse',
        'prediction',
        'std_error',
    ])

    # For each profile/form combo fit model
    for zolo_id in model_data['profile_name'].sort_values().unique():

        # Get data for profile/form
        temp_data = model_data[model_data['profile_name'] == zolo_id]

        # Grid search model params
        best_cfg, mse, best_model = lib.evaluate_models(temp_data['weight'].values, p_values, d_values, q_values)

        # Store meta information about models
        forecast = best_model.forecast()
        temp_df = pd.DataFrame({
            'profile_name': [zolo_id],
            'best_config': [best_cfg],
            'mse': [mse],
            'prediction': forecast[0],
            'std_error': forecast[1],
        })
        meta_df = pd.concat((meta_df, temp_df))

        logger.info('name_form: {} model complete'.format(zolo_id))

    # Calculate lower and upper bounds of CI
    meta_df['lower_bound'] = meta_df['prediction'] - (1.96 * meta_df['std_error'])
    meta_df['upper_bound'] = meta_df['prediction'] + (1.96 * meta_df['std_error'])

    # Add datetime field
    meta_df['forecast_start'] = forecast_start

    return meta_df


def load(meta_df):
    """
    Take the dataframe with diagnostic information and predictions and load to db
    :param meta_df: dataframe
    :return:
    """

    logger.info('Begin data load')

    # Create connection engine
    engine = create_engine('postgresql://{}:{}@{}/{}'.format(cfg['db_user_name'],
                                                             cfg['db_password'],
                                                             cfg['db_IP'],
                                                             cfg['db_name']))

    # Load to database
    meta_df.to_sql('model_meta', con=engine, if_exists='append', index=False)

    logger.info('Loading {} records to meta_df'.format(len(meta_df)))
    logger.info('Data load completed successfully')


# Main section
if __name__ == '__main__':
    main()
