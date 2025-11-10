from github import Github, Auth

# GitHub Token (keep confidential)
github_token = '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'

# GitHub configuration
repo_name = 'DWM-Project'
facttable_folder = "E2E_DWH_Pipeline/Fact Table Snapshot"

def upload_fact_table(file_content, file_name):
    # ✅ Use new recommended Auth method (prevents DeprecationWarning)
    auth = Auth.Token(github_token)
    g = Github(auth=auth)

    # ✅ Use full repo path with username
    repo = g.get_repo(f"AsifaSiraj/{repo_name}")
    file_path = f'{facttable_folder}/{file_name}'

    try:
        contents = repo.get_contents(file_path)
        repo.update_file(
            contents.path,
            f'Update {file_name}',
            file_content,
            contents.sha
        )
        print('✅ Fact Table Snapshot Updated')
    except Exception:
        repo.create_file(
            file_path,
            f'Create {file_name}',
            file_content
        )
        print('✅ Fact Table Snapshot Uploaded')
