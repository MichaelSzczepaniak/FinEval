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