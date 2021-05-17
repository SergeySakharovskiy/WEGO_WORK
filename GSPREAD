import gspread
from oauth2client.service_account import ServiceAccountCredentials

def open_gspread(rows_to_insert, doc_name, row_num):

    ''' Insert rows in the specified google sheet

    :param rows_to_insert: (list of lists)
    :param doc_name: name of g_spreadsheet (str)
    :param row_num: row position to insert rows (int)
    :return: insert rows in the specified google sheet
    '''

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open(doc_name).sheet1
    sheet.insert_rows(rows_to_insert,row_num,value_input_option='USER_ENTERED')
