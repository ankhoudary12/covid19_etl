"""Config containing parameters for various DAGs."""
import os

DAYS_TO_RECALCULATE = 7  # of days back to reset raw covid raw_data

COVID_DATA_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{}.csv"

# AWS
S3_BUCKET = os.environ.get("S3_BUCKET")
ACCESS_KEY_ID = os.environ.get("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("SECRET_ACCESS_KEY")
REGION = os.environ.get("REGION")

# Redshift Cluster Props

DWH_CLUSTER_TYPE = "multi-node"
DWH_NUM_NODES = 2
DWH_NODE_TYPE = "dc2.large"
DWH_IAM_ROLE_NAME = "RedshiftRole"
DWH_CLUSTER_IDENTIFIER = os.environ.get("DWH_CLUSTER_IDENTIFIER")

# Redshift DWH Props

DWH_DB = os.environ.get("DW_DB")
DWH_DB_USER = os.environ.get("DWH_DB_USER")
DWH_DB_PASSWORD = os.environ.get("DWH_DB_PASSWORD")
DWH_PORT = 5439
