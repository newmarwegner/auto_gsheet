import pandas as pd
import psycopg2
from decouple import config
from sheet_service import create_service

def con_postgres(dbname='geonode_data', user='sa_geonode'):

    return psycopg2.connect(f"dbname={config('dbname')} user={config('user')} host={config('host')} password={config('password')} port={config('port')}")


def call_sheets(service):
    sheet = service.spreadsheets()
    result_input = sheet.values().get(spreadsheetId=config('id'),
                                range='A1:AA1000').execute()

    return result_input.get('values', [])

def to_update(df):
    selection = df.loc[:,'reclassificacao'] != 'Não Avaliado'

    return df.loc[selection]

def insert_database(df):
    tabela = to_update(df)
    if tabela.empty:
        print('Sem novos updates para o banco!')
    else:

        print('inserindo dados')
        print(tabela)
    return


if __name__ == '__main__':
    values_input = call_sheets(create_service())[2:]
    df = pd.DataFrame(values_input[1:], columns=values_input[0]).iloc[:,:5]
    insert_database(df)