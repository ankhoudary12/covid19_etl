"""Module to delete a redshift cluster."""
import sys

import boto3

# make sure we can read in the config
sys.path.append("/usr/local/airflow/dags")
import config


def destroy_redshift_cluster(
    KEY: str, SECRET: str, AWS_REGION: str, DWH_CLUSTER_IDENTIFIER: str
) -> None:
    """Deletes a Redshift cluster.

    Args:
        KEY (str): aws access key
        SECRET (str): aws secret access key
        AWS_REGION (str): aws region
        DWH_CLUSTER_IDENTIFIER (str): name of redshift cluster to be deleted
    """
    redshift = boto3.client(
        "redshift",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name=AWS_REGION,
    )

    redshift.delete_cluster(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True
    )

    print("Deleting Redshift Cluster")


def main():
    """Deletes a redshift cluster."""
    KEY = config.ACCESS_KEY_ID
    SECRET = config.SECRET_ACCESS_KEY
    AWS_REGION = config.REGION
    DWH_CLUSTER_IDENTIFIER = config.DWH_CLUSTER_IDENTIFIER

    destroy_redshift_cluster(KEY, SECRET, AWS_REGION, DWH_CLUSTER_IDENTIFIER)


if __name__ == "__main__":
    main()
