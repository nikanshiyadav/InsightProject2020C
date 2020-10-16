import boto3
import boto3
import pandas as pd
import numpy as np
from IPython.display import display, Markdown
import seaborn as sns
%matplotlib inline
import matplotlib.pyplot as plt
glue = boto3.client('glue')
s3 = boto3.client('s3')

#list glue databases
def list_glue_databases():
    glue_database = glue.get_databases()

    for db in glue_database['DatabaseList']:
        print(db['Name'])

list_glue_databases()

#list tables
def list_glue_tables(database, verbose=True):
    glue_tables = glue.get_tables(DatabaseName=database)
    
    for table in glue_tables['TableList']:
        display(Markdown('**Table: ' + table['Name'] + '**'))
        display(Markdown('Location: ' + table['StorageDescriptor']['Location']))
        created = table['CreatedBy'].split('/')
        display(Markdown('Created by: ' + created[-1]))
        if verbose and created[-1] == 'AWS Crawler':
            display(Markdown(f'Records: {int(table["Parameters"]["recordCount"]):,}'))
            display(Markdown(f'Average Record Size: {table["Parameters"]["averageRecordSize"]} Bytes'))
            display(Markdown(f'Dataset Size: {float(table["Parameters"]["sizeKey"])/1024/1024:3.0f} MB'))
            display(Markdown(f'Crawler: {table["Parameters"]["UPDATED_BY_CRAWLER"]}'))
        if verbose:
            df_columns = pd.DataFrame.from_dict(table["StorageDescriptor"]["Columns"])
            display(df_columns[['Name', 'Type']])
            display(Markdown('---'))

list_glue_tables('taxicatalog', verbose=False)      


