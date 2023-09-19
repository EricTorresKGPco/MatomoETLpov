import os
import pandas as pd

#def extractDF() reads the data from the csv local file and returns a dataframe
def extractFD():
    return pd.read_csv('sample_data_ETL.csv')

#def sumEventsDF() transforms the dataframe by aggregating all rows with the same 'user_session' by
#summing the number of rows with the same 'user_session'
def sumEvents(df):
    return df.groupby(['user_session']).size().reset_index(name='Event_num')

#def concatEvents() concatenates all the 'event_type' values in the same 'user_session' in the order of 'event_time'
def concatEvents(df):
    return df.groupby(['user_session'])['event_type'].apply(lambda x: ' '.join(x)).reset_index(name='event_type')

#def SumAndConcatDF() calls the sumEventsDF() and concatEvents() functions and then merges the two dataframes
def SumAndConcatDF(df):
    return pd.merge(sumEvents(df), concatEvents(df), on='user_session')

#def contains_purchase() adds a row to the dataframe with the value '1' if the 'event_type' column contains 'purchase'
#and '0' if it does not
def contains_purchase(df):
    df['contains_purchase'] = df['event_type'].str.contains('purchase').astype(int)
    return df

#def transformDF() calls the SumAndConcatDF() and contains_purchase() functions
def transformDF(df):
    df = SumAndConcatDF(df)
    df = contains_purchase(df)
    return df

#def loadDF() loads the transformed dataframe into a csv file and then saves it in current working directory
def loadDF(df):
    cwd = os.getcwd()
    path = cwd + "/Behavior_Aggregate.csv"
    df.to_csv(path)
    return df

#def main() calls the extractDF(), transformDF(), and loadDF() functions
def main():
    df = extractFD()
    df = transformDF(df)
    return loadDF(df).to_json(orient='records')
