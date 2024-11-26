import pandas as pd
import json

# Input file path (original CSV with start_URL and redirect_chain columns)
input_path = '/content/redirect chain - Sheet1 (1).csv'  # Replace with your actual file path

# Load the original data
df = pd.read_csv(input_path)

# Ensure the first column is named 'start_URL'
df.rename(columns={df.columns[0]: "start_URL", df.columns[1]: "redirect_chain"}, inplace=True)

# Function to extract URLs from the JSON redirect chain
def extract_urls(redirect_chain):
    try:
        # Parse the JSON string into a Python object
        redirect_data = json.loads(redirect_chain)
        # Extract URLs from each entry in the redirect chain
        urls = [entry['url'] for entry in redirect_data]
        return urls
    except (json.JSONDecodeError, KeyError, TypeError):
        return []  # Return an empty list if there's an issue with the data

# Apply the function to create a new column with extracted URLs
df['extracted_urls'] = df['redirect_chain'].apply(extract_urls)

# Determine the maximum number of URLs in any redirect chain
max_urls = df['extracted_urls'].apply(len).max()

# Create column names for the URLs
url_columns = [f'url_{i+1}' for i in range(max_urls)]

# Expand the extracted URLs into separate columns
url_df = pd.DataFrame(df['extracted_urls'].to_list(), columns=url_columns)

# Combine the new columns with the original dataframe
processed_df = pd.concat([df[['start_URL']], url_df], axis=1)

# Create a new DataFrame for the transformed table
rows = []

# Iterate through each row in the processed DataFrame
for _, row in processed_df.iterrows():
    start_url = row['start_URL']
    # Collect all URLs from the row (excluding NaN)
    urls = row.dropna().to_list()[1:]  # Skip the start_URL column for redirect chain
    destination_url = urls[-1]  # The last URL in the chain (200 response code)

    # Add the start URL and each redirect as individual rows under the destination URL
    rows.append([start_url, destination_url])  # First row: start URL, destination URL
    for redirect_url in urls[:-1]:  # Add each intermediate redirect
        rows.append([redirect_url, destination_url])

# Create the final transformed DataFrame with the updated column order
transformed_df = pd.DataFrame(rows, columns=['redirect_URL', 'destination_URL'])

# Save the resulting DataFrame to a new CSV
output_path = 'redirect_chain_transformed.csv'
transformed_df.to_csv(output_path, index=False)

print(f"Transformed data saved to {output_path}")

from google.colab import auth
auth.authenticate_user()

import gspread
from google.auth import default
creds, _ = default()

gc = gspread.authorize(creds)
from gspread_dataframe import get_as_dataframe, set_with_dataframe

sh = gc.open_by_key('1K8bpDbO9FKSDS7MdFre9ZCYcQ3xlW4bJXEV4w_-XzAs') #https://docs.google.com/spreadsheets/d/1K8bpDbO9FKSDS7MdFre9ZCYcQ3xlW4bJXEV4w_-XzAs/edit?gid=1384898089#gid=1384898089

#Overwrithing the list of params and the count of URLs
worksheet = sh.worksheet("redirect_chain_reversed")
worksheet.clear()
set_with_dataframe(worksheet, transformed_df)
