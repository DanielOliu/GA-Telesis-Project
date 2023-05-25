import snowflake.connector
import numpy as np 
import logging
import datetime
import pandas as pd 
from snowflake.connector import connect
from decimal import Decimal
from email_send_function import *
from jinja2 import Template 
from openpyxl.styles import Font, Alignment

#creates a file to log any errors or actions that happen when code is ran
logging.basicConfig(filename='Run.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

try:
    current_time = datetime.datetime.now()
    logging.info(f'Code executed at: {current_time}')

    try: 

        ctx = connect ( 
        user = 'SAHILRATNAM', 
        password = 'C@t567328', 
        account = 'qr93446.east-us-2.azure',
        database = 'DW_PROD'
        ) 
        logging.info("Connected to snowflake database!")

    except Exception as e:
        logging.error(f'Could not connect to snowflake: {e}')



    sql= '''SELECT

            "P/N" PN

            , "Description" DESC

            , "P/N Group Code" GC

            , "P/N Application Code" AP

            , SUM("Qty On Hand") QOH

            , SUM("Qty Available") QA

            , CGN.GROUP_NAME GN

            , CMM.MIN MIN

            , CMM.MAX MAX

            FROM DW_PROD.DW.RPT_STOCK

            RIGHT OUTER JOIN CLA_PROD.MANUAL_TABLES.CAM_GROUP_NAME CGN ON CGN.PN = "P/N"

            RIGHT OUTER JOIN CLA_PROD.MANUAL_TABLES.CAM_MIN_MAX CMM ON CMM.GROUP_ID = CGN.GROUP_ID

            WHERE "Shop" = 'QCTL' AND "Warehouse Code" NOT LIKE '%SCRAP%'

            GROUP BY "P/N", "Description", "P/N Group Code", "P/N Application Code", GN, MIN, MAX
    ''' 
    cs = ctx.cursor()
    #logs if there is any error connecting to sql
    try:
        cs.execute(sql) 
    except Exception as e:
        logging.error(f"Error occured during execution of 'cs.execute': {e}")

    #creates data frame from the sql
    df1 = pd.DataFrame.from_records(iter(cs), columns=[x[0] for x in cs.description])

    num_rows = len(df1)
    num_cols = len(df1.columns)
    logging.info(f"Number of rows processed is {num_rows} rows and number of columns processed is {num_cols} columns")

    to = 'doliu@telesis.com' # recipient of the email 

    # Creates the table that contains the GN, the mean of the Min and Max, and the sums of the Qty on hand and Qty Avaiable values 
    df_QOH = df1.pivot_table(index= ['GN'], values = ['QOH'], aggfunc = np.sum).reset_index()
    df_QA = df1.pivot_table(index= ['GN'], values = ['QA'], aggfunc = np.sum).reset_index()
    df_mm = df1[['GN', 'MIN', 'MAX']].drop_duplicates().copy()

    df_new = df_QA.merge(df_QOH, left_on = 'GN', right_on = 'GN', how='left')
    df_new = df_new.merge(df_mm, left_on = 'GN', right_on = 'GN', how='left')

    df_new['QTB'] = np.where(df_new['QA'] <= df_new['MIN'], df_new['MAX'] - df_new['QA'], 0) #creates the quantity to buy column 

    # Creates 2 completed charts with all the values organized and duplicates being dropped for those that have exceeded the max and the ones that have gone below
    group_names_max = df_new.loc[df_new['QTB'] == 0,  'GN'].values
    completed_df_max = df1.loc[df1['GN'].isin(group_names_max), ['GN','PN', 'QOH', 'QA', 'MIN', 'MAX']].copy()
    completed_df_max = completed_df_max.merge(df_new[['GN', 'QTB']], on='GN', how='left')
    completed_df_max = completed_df_max[(completed_df_max['QOH'] != 0) & (completed_df_max['QA'] != 0)]
    completed_df_max = completed_df_max.sort_values('GN')

    group_names_min = df_new.loc[df_new['QTB'] > 0, 'GN'].values
    completed_df_min = df1.loc[df1['GN'].isin(group_names_min), ['GN','PN', 'QOH', 'QA', 'MIN', 'MAX']].copy()
    completed_df_min = completed_df_min.merge(df_new[['GN', 'QTB']], left_on='GN', right_on= 'GN', how='left')
    completed_df_min = completed_df_min[(completed_df_min['QOH'] != 0) & (completed_df_min['QA'] != 0)]
    completed_df_min = completed_df_min.sort_values('GN')

    # Puts both dataframes into a csv file and renames the columns to the correct titles
    completed_df_max.rename(columns = {'GN': 'Stage Name', 'QOH':'Qty On Hand', 'QA':'Qty Available', 'QTB':'Qty To Buy'}, inplace=True)
    completed_df_min.rename(columns = {'GN': 'Stage Name', 'QOH':'Qty On Hand', 'QA':'Qty Available', 'QTB':'Qty To Buy'}, inplace=True)

    completed_df_min.to_excel('QuantityToBuy.xlsx', index=False)
    completed_df_max.to_excel('MaximumAlert.xlsx', index = False)

    #change width of columns in Maximum Alert sheet
    df = pd.read_excel('MaximumAlert.xlsx')  # Replace 'your_file.xlsx' with the actual file name and path

    # Group the PN values by Stage Name and join them with commas
    df_grouped = df.groupby('Stage Name').agg({'PN': ', '.join, 'Qty On Hand': 'first', 'Qty Available': 'first', 'MIN': 'first', 'MAX': 'first', 'Qty To Buy': 'first'}).reset_index()

    # Create a new Excel file
    output_file = 'MaximumAlert.xlsx'  # Replace 'your_updated_file.xlsx' with the desired output file name and path
    writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

    # Write the grouped data to the Excel file
    df_grouped.to_excel(writer, sheet_name='Sheet1', index=False)

    # Adjust the column width of columns A to G
    worksheet = writer.sheets['Sheet1']
    worksheet.set_column('B:B', 93)
    worksheet.set_column('A:A', 30)
    worksheet.set_column('C:G', 30)
    writer.save()


    #html template of a table for email body
    template = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Data Table</title>
        <style>
            table {
                border-collapse: collapse;
                width: 100%;
            }
            
            th, td {
                padding: 8px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }
            
            th {
                background-color: #f2f2f2;
            }

            #mintable tr td:first-child {
                font-weight: bold;
            }
        </style>
    </body>
    </html>
    '''

    # An if statement for if there is no product that went below the MIN value it sends an email without the products needed to buy to fix the supply
    if not completed_df_min.empty:

        read = pd.read_excel('QuantityToBuy.xlsx')
        html_table = read.to_html(index=False, justify = 'right', border=.5)
        html_content = html_table + template
        
        body = f'''<p>Dear User,</p>

        <p> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Above is a file that contains the products in a table that have either reached or exceeded the maximum quantity organized by Stage number. Below displays a table about the products that need to increase in supply.</p>

                {html_table}
        
        '''
        subject = 'Minimum and Maximum Update!'
        try:
            send_my_email(to, subject, body, 'MaximumAlert.xlsx', body_type = 'html')
            current_time = datetime.datetime.now()
            logging.info(f"Email has been sent to {to} at {current_time}. ")
        except Exception as e:
            logging.error(f'Error! Could not send email: {e}')
    else:

        body = '''Dear User,

            Above is a file that contains the products in a table that have either reached or exceeded the maximum quantity organized by Stage number.
                '''
        
        subject = 'Max Update!'
        try:
            send_my_email(to, subject, body, 'MaximumAlert.xlsx', body_type = 'plain')
            current_time = datetime.datetime.now()
            logging.info(f"Email has been sent to {to} at {current_time}. ")
        except Exception as e:
            logging.error(f'Error! Could not send email: {e}')
    end_time = datetime.datetime.now()     
    logging.info(f'Code execution finished at: {end_time}.')
    execution_time = end_time - current_time
    logging.info(f"The code took {execution_time} seconds to complete.")       


except Exception as e:
    logging.error(f"Error occured: {e}")