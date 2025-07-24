import numpy as np
import pandas as pd
import yfinance as yfinance
import re
from datetime import datetime as dt
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
        The following docling modules must be available in the environment:
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


def get_vang_statement_date(parsed_statement_as_markdown,
                            id_text=", quarter-to-date statement"):
    """ Extracts the end-of-month statement date from a Vanguard monthly PDF
    statement that has been parsed into markdown format.
    
    Args:
      parsed_statement_as_markdown (str): Vanguard statement parsed into 
        markdown format
      id_test (str): statement text identifying the line which the statement
        date resides
    Returns:
      str: end-of-month statement date in YYYY-MM-DD format
    
    """
    report_lines = parsed_statement_as_markdown.split('\n')
    statement_date_line = "DATE NOT FOUND!!!"
    for line in report_lines:
        line_has_date = id_text in line
        if line_has_date:
            statement_date_line = line
            break

    # get the date in YYYY-MM-DD format
    statement_date = statement_date_line.replace(id_text, "")
    statement_date = dt.strptime(statement_date.replace("## ", ""), "%B %d, %Y")
    statement_date = statement_date.strftime("%Y-%m-%d")

    return statement_date


def get_vang_stock_table_segs(report_lines,
                              file_type = 'markdown',
                              header_regex = r"^[|]\s+Symbol\s+[|]\s+Name\s+[|]\s+Quantity\s+[|]\s+Price on "):
    """ Finds the row numbers of the start and end of each stock table segment in report_lines

    Args:
      report_lines (list[str]): list of strings that are lines from the parsed pdf report
      file_type (str): file type to be processed, default: markdown
      header_regex (str): regular expression identifying the header row of each stock table segment
        in the statement

    Returns:
      tuple of 2-tuples: First item in each tuple is the row number of first row
      of a stock table segment. Second item in each tuple is the row number of the
      last row of a stock table segment
    """
    last_line_table_seg = []
    stock_table_headers = []
    stock_table_last_lines = []
    found_table_header = False
    found_table_end = None
    for position, line in enumerate(report_lines):
        header_search_result = re.search(header_regex, line)
        if header_search_result is not None:                # found table header
            found_table_header = True
            stock_table_headers.append((position, header_search_result))
        elif found_table_header and line.startswith("| "):  # row of table segment
            continue
        elif found_table_header and len(line) == 0:         # empty line after table
            stock_table_last_lines.append(position - 1)
            found_table_header = False  # look for next segment

    header_indices = [x[0] for x in stock_table_headers]
    # print(f"header rows of table segments: {header_indices}")
    # print(f"last rows of table segments: {stock_table_last_lines}")   
    table_segs = tuple(zip(header_indices, stock_table_last_lines))

    return table_segs


def make_vang_stock_record_dict(row_string, delimiter='|'):
    """ Converts a delimited string into a record dict which can
    then be used build a data frame

    Args:
      row_string (str): record of a security holding in the statement
      delimiter (str): delimiter used to separate values in row_string

    Returns:
      dict: with the following keys representing a field in the record:
        statement_date - date of last day in the statement formatted YYYY-MM-DD
        symbol - ticker symbol for the security
        name - name of the security
        quantity - number of shares held
        price_statement_eom - price on the last day of statement
        balance_statement_eom - number of shares of security held on statement_date
    
    """
    table_line_tokens = row_string.split('|')
    # parse row into record dict
    table_row_dict = {
        "statement_date": table_line_tokens[0],
        "symbol": table_line_tokens[1],
        "name": table_line_tokens[2],
        "quantity": table_line_tokens[3],
        "price_statement_eom": table_line_tokens[4],
        "balance_statement_eom": table_line_tokens[6]
    }

    return table_row_dict



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
