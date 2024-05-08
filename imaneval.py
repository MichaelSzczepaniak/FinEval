import numpy as np
import pandas as pd
import yfinance as yfinance

def get_eomonth_price(df, start_month='2019-01', end_month='prior', price_col='Close'):
    """
    Determines the last day of every month in df['Date'] and returns a dataframe
    with two columns: Date and whatever is specified by the price_col parameter
    with just the rows from the trading data of the month.
    
    Args:
    df (pandas.core.frame.DataFrame): dataframe of daily prices for a security
        with at least 2 columns: Date and whatever is designated by the
        price_col parameter
    start_month (str): month of the form yyyy-mm designating the first month of
        of prices to be returned
    end_month (str): month of the form yyyy-mm designating the last month of
        of prices to be returned. If 'prior' (default) is specified, the month
        prior to the current month is used.
    price_col (str): name of the price column to determine the value of on the
        last date of the month
    
    Returns:
    pandas.core.frame.DataFrame with two columns: Date and value set for price_col
    
    
    """
    df_return = df.copy()
    df_return['year'] = df_return.index.year.astype('string')
    df_return['month'] = df_return.index.month.astype('string')
    df_return['month'] = df_return['month'].str.zfill(2)  # zero pad
    df_return['year_mo'] = df_return['year'] + '-' + df_return['month']
    
    # build date column, make it the index, drop the extra data column
    df_return['day'] = df_return.index.day
    df_return['date'] = df_return.index.date
    df_return.index = df_return['date']
    df_return.drop(['date'], axis=1, inplace=True)
    
    # convert index from datetime to string so we can join on it
    df_return.index = df_return.index.astype('string')
    df_price = df_return[[price_col, 'day', 'year_mo']]
    
    # find the last trading day of the month
    df_id_eom = df_price.groupby('year_mo')['day'].max('day')
    df_id_eom = pd.DataFrame(df_id_eom)
    df_id_eom['date'] = df_id_eom.index + '-' + df_id_eom['day'].astype('string').str.zfill(2)
    
    # use df_id_oem to filter out all but last trading day of month
    df_eom = df_price.merge(df_id_eom, on='date')
    df_eom = df_eom[['date', price_col]]
    
    # filter between start_month and end_month TODO
    
    return(df_eom)