import requests
from github import Github, Auth, GithubException
import pandas as pd
from io import StringIO

# -------------------------------
#  Configuration
# -------------------------------
mockaroo_api_key = '7df0f660'  # Mockaroo API Key

# ⚠️ Always store tokens securely in .env in production — for now, keep inline for local testing
github_token = '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'  # Your GitHub PAT
repo_name = 'AsifaSiraj/DWM-Project'  # e.g., username/repository
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
    try:
        # ✅ Authenticate safely
        auth = Auth.Token(github_token)
        g = Github(auth=auth)

        # ✅ Verify credentials before proceeding
        user = g.get_user().login
        print(f"🔐 Authenticated as: {user}")

        # ✅ Access repo
        repo = g.get_repo(repo_name)

        file_path = f'{dataset_folder}/{file_name}'
        try:
            contents = repo.get_contents(file_path)
            prev_content = contents.decoded_content.decode('utf-8')
            appended_content = append_csv(prev_content, file_content)
            repo.update_file(contents.path, f'Update {file_name}', appended_content, contents.sha)
            print(f"✅ Updated: {file_name}")
        except GithubException as e:
            if e.status == 404:
                # File does not exist yet — create new
                repo.create_file(file_path, f'Create {file_name}', file_content)
                print(f"🆕 Uploaded new file: {file_name}")
            else:
                raise e
    except GithubException as e:
        print(f"❌ GitHub API error: {e.data.get('message', str(e))}")
    except Exception as e:
        print(f"❌ Unexpected error in upload_to_github: {e}")


# -------------------------------
#  Main Script
# -------------------------------
def main():
    print("🚀 Starting dataset upload process...")
    for schema in schemas:
        print(f'\n📦 Fetching data for schema: {schema}')
        try:
            dataset = get_data(schema)
            file_name = f'{schema}.csv'
            print(f'⬆️ Uploading {file_name} to GitHub...')
            upload_to_github(dataset.decode('utf-8'), file_name)
        except requests.exceptions.RequestException as re:
            print(f"⚠️ Error fetching schema {schema}: {re}")
        except Exception as e:
            print(f"⚠️ Unexpected error for {schema}: {e}")
    print('\n✅ All datasets processed.')


if __name__ == '__main__':
    main()
