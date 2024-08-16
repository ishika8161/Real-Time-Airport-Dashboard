import csv

with open('flights.csv', 'r') as file:
    reader = csv.reader(file)
    headers = next(reader)
    print(headers)
