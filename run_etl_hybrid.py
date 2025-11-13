# """Runner script to execute the ETL in hybrid mode.

# Usage:
#   - Ensure your venv is activated and (optionally) set `MOCKAROO_API_KEY` in the environment.
#   - Optionally set DB params via env vars or edit defaults below.

# This script calls `etl_master(source='hybrid', db_params=...)`.
# """
# import os
# from E2E_DWH_Pipeline.Pipeline_Support.ETL_MasterFunction import etl_master

# # Read DB params from environment with sensible defaults used in prior runs
# DB_PARAMS = {
#     'user': os.getenv('DB_USER', 'postgres'),
#     'password': os.getenv('DB_PASSWORD', 'asifa123'),
#     'host': os.getenv('DB_HOST', 'localhost'),
#     'port': int(os.getenv('DB_PORT', 5432)),
#     'db_name': os.getenv('DB_NAME', 'Real-Estate-Management')
# }

# if __name__ == '__main__':
#     print('Running ETL (hybrid) with DB params:', {k: v for k, v in DB_PARAMS.items() if k != 'password'})
#     etl_master(source='hybrid', db_params=DB_PARAMS)
