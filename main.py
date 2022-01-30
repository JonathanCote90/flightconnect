from google.cloud import bigquery
import pandas as pd

#client = bigquery.Client()
table_id = 'asymmetric-rite-339601.Flights_or_whatever.actual_departure'

page_url = 'https://flightaware.com/live/flight/ACA426/history'
page_table = pd.read_html(page_url)[3].iloc[:-1]

pandas_departure = pd.to_datetime(page_table['Date'] + " "  + page_table['Departure'].str[0:-4])
pandas_departure_tz = pd.DataFrame()
pandas_departure_tz['actual_departure'] = pandas_departure.dt.tz_localize(page_table['Departure'].iloc[0][-3:])
pandas_departure_tz['flight_number'] = page_url.split('/')[-2]
pandas_departure_tz['actual_departure'] = pandas_departure_tz['actual_departure'].dt.tz_convert('UTC')#.astype(int)/1e9 #.dt.tz_localize('UTC').dt.timestamp

pandas_departure_tz.to_gbq(table_id,if_exists = 'append')# table_schema = [{'name' :'actual_departure', 'type' : 'TIMESTAMP'},
#{'name' : 'flight_number', 'type' : 'STRING'}])

