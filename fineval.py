import numpy as np
import pandas as pd
from datetime import datetime as dt
import yfinance as yfinance
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend

def parse_vang_pdf(pdf_path, return_type='markdown'):
    """ Parses a Vanguard monthly PDF statement and returns a result
    specified by return_type (default is markdown)

    Args:
      pdf_path (str): path to the pdf statement file to be parsed
      return_type (str): type of file to be returned (default: markdown)

    Return:
      str: the file specified by pdf_path in the format specified by
      return_type

    Prerequisites:
        The following docling module must be available in the environment:
        docling.document_converter.DocumentConverter
        docling.document_converter.PdfFormatOption
        docling.datamodel.base_models.InputFormat
        docling.datamodel.pipeline_options.PdfPipelineOptions
        docling.backend.pypdfium2_backend.PyPdfiumDocumentBackend
    """
    
    pipeline_options = PdfPipelineOptions(
        do_ocr=False,  # Set True if you suspect any scanned/image pages
        do_table_structure=True  # Enables table extraction
    )

    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options,
                backend=PyPdfiumDocumentBackend
            )
        }
    )

    result = converter.convert(pdf_path)
    result_converted = ""
    if return_type == 'markdown':
        result_converted = result.document.export_to_markdown()
    # elif:  # TODO add other types

    return result_converted

def get_eomonth_price(df, start_month='2019-01', end_month='prior', price_col='Close'):
    """
    Determines the last day of every month in df['Date'] and returns a dataframe
    with two columns: Date and whatever is specified by the price_col parameter
    with just the rows from the trading data of the month.
    
    Args:
    df (pandas.core.frame.DataFrame): dataframe of daily prices for a security
        with at least 2 columns: an index which is a pandas Timestamp (i.e.
        pandas._libs.tslibs.timestamps.Timestamp) and a price that is designated
        by the price_col
    start_month (str): month of the form yyyy-mm designating the first month of
        of prices to be returned
    end_month (str): month of the form yyyy-mm designating the last month of
        of prices to be returned or 'prior'. If 'prior' (default) is specified,
        the month prior to the current month is used.
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
    df_eom = df_eom[['date', price_col, 'year_mo']]
    
    # calc the prior month if needed
    if end_month == 'prior':
        # get the current month so we can calc the prior month
        end_month = str(dt.today().year) + "-" + \
                    str(dt.today().month - 1).zfill(2)
    # filter between start_month and end_month
    df_return = df_eom.loc[(df_eom['year_mo'] >= start_month) & \
                           (df_eom['year_mo'] <= end_month), :]
    
    return(df_return)


def get_eomonth_sp500(start_month='2019-01', end_month='prior'):
    """
    
    
    
    """
    spx = yf.Ticker("^SPX")
    df_ticker = spx.history(period="max", interval="1d",
                            start="2019-01-01", end="2024-05-02",
                            auto_adjust=True, rounding=True)
    
    return(None)
