from kafka import KafkaConsumer
from threading import Thread
from dash import Dash, html, dash_table
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc
import pandas as pd

# Global data structure to store messages
flight_data = []

# Kafka Consumer Setup
consumer = KafkaConsumer('flight_updates', bootstrap_servers='localhost:9092')

# Function to consume messages and update the shared data structure
def consume_messages():
    global flight_data
    for message in consumer:
        decoded_message = message.value.decode('utf-8')
        flight_info = decoded_message.split(',')
        flight_data.append({
            'date': flight_info[0],
            'flight_number': flight_info[1],
            'origin': flight_info[2],
            'destination': flight_info[3],
            'dep_delay': flight_info[4],
            'arr_delay': flight_info[5],
            'cancelled': flight_info[6]
        })
        print(f"Consumed: {decoded_message}")

# Run the consumer in a separate thread
consumer_thread = Thread(target=consume_messages)
consumer_thread.start()

# Create a Dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Layout for the dashboard
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H1("Real-Time Airport Notifications Dashboard"), className="text-center")
    ]),
    dbc.Row([
        dbc.Col(dash_table.DataTable(
            id='flight-table',
            columns=[
                {"name": "Date", "id": "date"},
                {"name": "Flight Number", "id": "flight_number"},
                {"name": "Origin", "id": "origin"},
                {"name": "Destination", "id": "destination"},
                {"name": "Departure Delay (min)", "id": "dep_delay"},
                {"name": "Arrival Delay (min)", "id": "arr_delay"},
                {"name": "Cancelled", "id": "cancelled"},
            ],
            data=[],  # Empty initially
            style_table={'overflowX': 'auto'},
            style_cell={'textAlign': 'center'},
            style_header={'backgroundColor': 'black', 'color': 'white'},
            style_data_conditional=[
                {'if': {'filter_query': '{cancelled} = "1.00"'},
                 'backgroundColor': 'red',
                 'color': 'white'}
            ],
        ))
    ])
])

# Callback to update the table every second
@app.callback(
    Output('flight-table', 'data'),
    [Input('flight-table', 'data')]
)
def update_table(rows):
    global flight_data
    # Convert the flight data list to a DataFrame for display
    df = pd.DataFrame(flight_data)
    return df.to_dict('records')

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True, use_reloader=False)
