import pandas as pd
from decouple import config
from sheet_service import create_service

def call_sheets(service):
    sheet = service.spreadsheets()
    result_input = sheet.values().get(spreadsheetId=config('id'),
                                range='A1:AA1000').execute()

    return result_input.get('values', [])



if __name__ == '__main__':
    values_input = call_sheets(create_service())
    # print(values_input)
    df = pd.DataFrame(values_input[1:], columns=values_input[0])
    filtro = df.loc[:,'Nome'] == 'Newmar'
    print(df.loc[filtro])
