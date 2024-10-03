import requests
import pandas as pd
from io import StringIO

# Replace the demo API key with your own if necessary
url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=Z6UTWLY388VTURV5&datatype=csv'
r = requests.get(url)

# Use StringIO to treat the response content as a file-like object
data = StringIO(r.text)

# Read the CSV data into a DataFrame
df = pd.read_csv(data)

# Display the DataFrame
print(df)