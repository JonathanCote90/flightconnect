from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()
table_id = 'asymmetric-rite-339601.Flights_or_whatever.First_flights'

page_url = 'https://flightaware.com/live/flight/ACA426/history'
page_table = pd.read_html(page_url)[3].iloc[:-1]

pandas_departure = pd.to_datetime(page_table['Date'] + " "  + page_table['Departure'].str[0:-4])
print(page_table['Departure'].iloc[0][-3:])
pandas_departure_tz = pd.DataFrame(pandas_departure).apply(lambda x:x.tz_localize(page_table['Departure'].iloc[0][-3:]))
pandas_departure_tz.columns = [['actual_departure']]#,'actual_arrival']]
print(pandas_departure_tz)
#pd.options.display.max_columns = 10 
pandas_departure_tz.index.name = 'Sebastien'
job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("actual_departure", bigquery.enums.SqlTypeNames.TIMESTAMP)
        ]
    )
pandas_departure_tz['actual_departure'] = pandas_departure_tz['actual_departure'].dt.tz_localize('UTC')

#pandas_departure_tz.to_gbq(table_id,if_exists = 'append', table_schema = [{'name' :'actual_departure', 'type' : 'TIMESTAMP'}])
#job = client.load_table_from_dataframe(pd.DataFrame(pandas_departure_tz), table_id)  # Make an API request.
#job.result()  # Wait for the job to complete.


