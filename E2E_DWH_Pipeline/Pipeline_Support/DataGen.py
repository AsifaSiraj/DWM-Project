import requests
from github import Github, Auth
import pandas as pd
from io import StringIO

# -------------------------------
#  Configuration
# -------------------------------
mockaroo_api_key = '7df0f660'  # Mockaroo API Key
github_token = ''  # GitHub Token
repo_name = 'AsifaSiraj/DWM-Project'  # Full repository path including owner
dataset_folder = 'Database/Datasets'

# Mockaroo schemas
schemas = [
    'address', 'client', 'agent', 'owner', 'features',
    'property', 'maintenance', 'visit', 'commission',
    'sale', 'contract', 'rent', 'admin'
]

# -------------------------------
#  Helper Functions
# -------------------------------
def get_data(schema):
    """Fetch data from Mockaroo API."""
    url = f'https://my.api.mockaroo.com/{schema}.csv?key={mockaroo_api_key}'
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def append_csv(prev_content, new_content):
    """Append new data to existing CSV content."""
    prev_df = pd.read_csv(StringIO(prev_content))
    new_df = pd.read_csv(StringIO(new_content))
    combined_df = pd.concat([prev_df, new_df], ignore_index=True)
    return combined_df.to_csv(index=False)

def upload_to_github(file_content, file_name):
    """Upload or update file on GitHub repository."""
    auth = Auth.Token(github_token)
    g = Github(auth=auth)

    #  FIXED LINE HERE
    repo = g.get_repo(repo_name)

    file_path = f'{dataset_folder}/{file_name}'
    try:
        contents = repo.get_contents(file_path)
        prev_content = contents.decoded_content.decode('utf-8')
        appended_content = append_csv(prev_content, file_content)
        repo.update_file(contents.path, f'Update {file_name}', appended_content, contents.sha)
        print(f" Updated: {file_name}")
    except Exception:
        repo.create_file(file_path, f'Create {file_name}', file_content)
        print(f" Uploaded: {file_name}")

# -------------------------------
#  Main Script
# -------------------------------
def main():
    for schema in schemas:
        print(f' Fetching data for schema: {schema}')
        dataset = get_data(schema)
        file_name = f'{schema}.csv'
        print(f' Uploading {file_name} to GitHub...')
        upload_to_github(dataset.decode('utf-8'), file_name)
    print(' All datasets have been uploaded successfully.')

if __name__ == '__main__':
    main()
