from dbhpgm.dbConfig import dbConnector

# Connect to CH and get query result
def get_query_results(QUERY, project_id, page_condition, start_date, end_date, device_id):
   engineCH = dbConnector('chatamart', project_id=project_id)
   result_db = engineCH.getSQLResult(QUERY.format(project_id=project_id,
                                                  page_condition=page_condition,
                                                  start_date=start_date,
                                                  end_date=end_date,
                                                  device_id=device_id))
   return result_db


# Clean data
import pandas as pd
def clean_data (result_db):
    df = pd.DataFrame(data=result_db)
    df2 = df.dropna()
    df2.drop_duplicates(inplace=True)
    df2.columns = ['session_id',
                   'session_number_of_views',
                   'dom_interactive_after_msec',
                   'session_number_of_transactions',
                   'is_last',
                   'scroll_rate',
                   'session_duration',
                   'activity_rate'
                   ]
    return(df2)

# To define the independent variable (convert from input in integer type into string)
def independant_var (indep_var):
    if indep_var == 1:
        indep_var2='dom_interactive_after_msec'
    if indep_var == 2:
        indep_var2='scroll_rate'
    if indep_var == 3:
        indep_var2='session_duration'
    if indep_var == 4:
        indep_var2 = 'session_number_of_views'
    if indep_var == 5:
        indep_var2 = 'activity_rate'
    return indep_var2

# Create two groups to compare and realize the statistic test
from scipy.stats import stats as stats
def stat_test(df2, dep_var,indep_var2):
    if dep_var == 1:
        df_gp1 = df2[df2['session_number_of_views'] == 1]
        df_gp2 = df2[df2['session_number_of_views'] > 1]
        return stats.ttest_ind(df_gp1[indep_var2], df_gp2[indep_var2])
    if dep_var == 2:
        df_gp1 = df2[df2['is_last'] == 1]
        df_gp2 = df2[df2['is_last'] == 0]
        return stats.ttest_ind(df_gp1[indep_var2], df_gp2[indep_var2])
    if dep_var == 3:
        df_gp1 = df2[df2['session_number_of_transactions'] == 1]
        df_gp2 = df2[df2['session_number_of_transactions'] == 0]
        return stats.ttest_ind(df_gp1[indep_var2], df_gp2[indep_var2])

# Results interpretation
def interpretation(results):
    pvalue=float(results[1])

    if pvalue < 0.1:
        print(f'The difference is significant (pvalue= {round(pvalue,5)}, coef.={results[0]}). We can suggest an effect of the IV on the DV.')
    else:
        print(f'The difference is not significant (pvalue= {round(pvalue,5)}, coef.={results[0]}).')


def get_DV_from_input(indep_var):
    if indep_var==1:
        #loadtime
        input_select = """multiIf((dom_interactive_after_msec >0 AND dom_interactive_after_msec<1000), '1.between 0 & 1 second', (dom_interactive_after_msec >=1000 AND dom_interactive_after_msec<2000), '2.between 1 & 2 seconds', (dom_interactive_after_msec >=2000 AND dom_interactive_after_msec<3000), '3.between 2 & 3 seconds',(dom_interactive_after_msec >=3000) ,'4. more than 3 seconds', null) as Loading_Time,"""

        #scroll_rate
    if indep_var==2:
        input_select="""multiIf(scroll_rate is null, 'null', scroll_rate < 10, '01. <10%',scroll_rate < 20, '02. <20%', scroll_rate < 30, '03. <30%', scroll_rate < 40, '04. <40%', scroll_rate < 50, '05. <50%', scroll_rate < 60, '06. <60%', scroll_rate < 70, '07. <70%', scroll_rate < 80, '08. <80%', scroll_rate < 90, '09. <90%', '10. <100%') AS Scroll_Rate,"""

        #view_duration
    if indep_var==3:
        input_select="""multiIf((view_duration_msec >0 AND view_duration_msec <5000), '1. Less than 5 seconds',(view_duration_msec >5000 AND session_duration_msec <15000), '2. Between 5 & 15 seconds',(view_duration_msec >15000 AND session_duration_msec <30000), '3. Between 15 & 30 seconds',(view_duration_msec >30000 AND view_duration_msec <45000), '4. Between 30 & 45 seconds',(view_duration_msec >45000), '5. More than 45 seconds', 'null') as Time_Spent,"""

        #nb of pages viewed
    if indep_var == 4:
        input_select ="""multiIf((session_number_of_views >0 AND session_number_of_views <5), '1. Less than 5 pages',(session_number_of_views >5 AND session_number_of_views <10), '2. Between 5 & 10 pages',(session_number_of_views >10 AND session_number_of_views <15), '3. Between 10 & 15 pages',(session_number_of_views >15 AND session_number_of_views <20), '4. Between 15 & 20 pages',(session_number_of_views >20), '5. More than 20 pages', 'null') as Nb_Pages_Viewed,"""

    if indep_var == 5:
        input_select=99

    return input_select

def get_DVname_from_input(indep_var):
    if indep_var==1:
        input_select_name = """Loading_Time"""

    if indep_var==2:
        input_select_name= """Scroll_Rate"""

    if indep_var==3:
        input_select_name= """Time_Spent"""

    if indep_var==4:
        input_select_name="""Nb_Pages_Viewed"""

    if indep_var==5:
        input_select_name=99

    return input_select_name

def get_query_results_to_viz(QUERY, project_id, page_condition, start_date, end_date, device_id, input_select, input_select_name):
   engineCH = dbConnector('chatamart', project_id=project_id)
   data = engineCH.getSQLResult(QUERY.format(project_id=project_id,
                                                  page_condition=page_condition,
                                                  start_date=start_date,
                                                  end_date=end_date,
                                                  device_id=device_id,
                                                  input_select=input_select,
                                                  input_select_name=input_select_name))
   return data



# Connect to Gsheet
import gspread

from gspread.models import Spreadsheet
from oauth2client.service_account import ServiceAccountCredentials

# Connect to gsheet
def connect_gsheet(sheet_id):
   scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']
   creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
   gc = gspread.authorize(creds)
   return gc.open_by_key(sheet_id)

# Write gsheet
def write_gsheet(GSHEET_CONN, query_result, tab_name):
   wks = GSHEET_CONN.worksheet(tab_name)  # tab name
   wks.clear()
   wks.update([query_result.columns.values.tolist()] + query_result.values.tolist())

