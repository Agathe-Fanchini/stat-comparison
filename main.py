
import pandas as pd
from dbhpgm.dbConfig import dbConnector
from gspread.models import Spreadsheet
from oauth2client.service_account import ServiceAccountCredentials
from scipy.stats import stats
from functions import *
from input import *
from queries import *

if __name__ == "__main__":
   result_db = get_query_results(QUERY=QUERY_1,
                                 project_id=project_id,
                                 page_condition=page_condition,
                                 start_date=start_date, 
                                 end_date=end_date,
                                 device_id=device_id)
   df2=clean_data(result_db)
   indep_var2=independant_var(indep_var)

   results=stat_test(df2, dep_var,indep_var2)
   interpretation(results)


   input_select=get_DV_from_input(indep_var)
   input_select_name=get_DVname_from_input(indep_var)

   if dep_var == 1:
       if indep_var==5:
           data = get_query_results_to_viz(QUERY=QUERY_activity_bouncers,
                                           project_id=project_id,
                                           page_condition=page_condition,
                                           start_date=start_date,
                                           end_date=end_date,
                                           device_id=device_id,
                                           input_select=input_select,
                                           input_select_name=input_select_name)
           GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
           write_gsheet(GSHEET_CONN, data, 'Bouncers')

       else:
           data = get_query_results_to_viz(QUERY=QUERY_bouncers,
                                        project_id=project_id,
                                        page_condition=page_condition,
                                        start_date=start_date,
                                        end_date=end_date,
                                        device_id=device_id,
                                        input_select=input_select,
                                        input_select_name=input_select_name)
           GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
           write_gsheet(GSHEET_CONN, data, 'Bouncers')

   if dep_var == 2:
       if indep_var == 5:
           data = get_query_results_to_viz(QUERY=QUERY_activity_exiters,
                                           project_id=project_id,
                                           page_condition=page_condition,
                                           start_date=start_date,
                                           end_date=end_date,
                                           device_id=device_id,
                                           input_select=input_select,
                                           input_select_name=input_select_name)
           GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
           write_gsheet(GSHEET_CONN, data, 'Exiters')
       else:
           data = get_query_results_to_viz(QUERY=QUERY_exiters,
                                           project_id=project_id,
                                           page_condition=page_condition,
                                           start_date=start_date,
                                           end_date=end_date,
                                           device_id=device_id,
                                           input_select=input_select,
                                           input_select_name=input_select_name)
           GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
           write_gsheet(GSHEET_CONN, data, 'Exiters')

   if dep_var == 3:
       if indep_var == 5:
           data = get_query_results_to_viz(QUERY=QUERY_activity_transaction,
                                           project_id=project_id,
                                           page_condition=page_condition,
                                           start_date=start_date,
                                           end_date=end_date,
                                           device_id=device_id,
                                           input_select=input_select,
                                           input_select_name=input_select_name)
           GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
           write_gsheet(GSHEET_CONN, data, 'Transactions')

       else:
           data = get_query_results_to_viz(QUERY=QUERY_transaction,
                                           project_id=project_id,
                                           page_condition=page_condition,
                                           start_date=start_date,
                                           end_date=end_date,
                                           device_id=device_id,
                                           input_select=input_select,
                                           input_select_name=input_select_name)
           GSHEET_CONN = connect_gsheet(sheet_id=sheet_id)
           write_gsheet(GSHEET_CONN, data, 'Transactions')

   print(f'Complete, go to https://docs.google.com/spreadsheets/d/{sheet_id}/edit?usp=sharing')








