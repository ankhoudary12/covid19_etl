"""Module to create a redshift cluster."""
import json
import time
import sys
from typing import Any

import boto3
import psycopg2

# make sure we can read in the config
sys.path.append("/usr/local/airflow/dags")
import config


def create_iam_role(
    ACCESS_KEY_ID: str, SECRET_ACCESS_KEY: str, REGION: str, DWH_IAM_ROLE_NAME: str
) -> str:
    """Function to generate IAM role for Redshift cluster so it can interact with S3.

    Args:
        KEY (str): aws access key
        SECRET (str): aws secret access key
        AWS_REGION (str): aws region
        DWH_IAM_ROLE_NAME (str): name of IAM role to be created

    Returns:
        roleArn (str): ARN of newly created IAM role
    """
    # create the iam boto3 client

    iam = boto3.client(
        "iam",
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        region_name=REGION,
    )

    try:
        print("1.1 Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path="/",
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")

    iam.attach_role_policy(
        RoleName=config.DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]

    return roleArn


def create_redshift_cluster(
    ACCESS_KEY_ID: str,
    SECRET_ACCESS_KEY: str,
    REGION: str,
    DWH_CLUSTER_IDENTIFIER: str,
    DWH_CLUSTER_TYPE: str,
    DWH_NODE_TYPE: str,
    DWH_NUM_NODES: int,
    DWH_DB: str,
    DWH_DB_USER: str,
    DWH_DB_PASSWORD: str,
    roleArn: str,
):
    """Function to create a new redshift cluster.

    Args:
        KEY (str): aws access key
        SECRET (str): aws secret access key
        AWS_REGION (str): aws region
        DWH_CLUSTER_IDENTIFIER (str): name of cluster role to be created
        DWH_CLUSTER_TYPE (str): type of cluster (multi-node usually)
        DWH_NODE_TYPE (str): node type for redshift cluster
        DWH_NUM_NODES (int): how many nodes to use in cluster
        DWH_DB (str): database name
        DWH_DB_USER (str): username for db
        DWH_DB_PASSWORD (str): password for db
        roleArn (str): IAM role ARN for redshift cluster

    Returns:
        redshift: the redshift object and
        cluster_props: redshift cluster properties
    """
    # create the redshift boto3 client

    redshift = boto3.client(
        "redshift",
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        region_name=REGION,
    )

    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            # Roles (for s3 access)
            IamRoles=[roleArn],
        )

    except Exception as e:
        print(e)

    cluster_props = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]

    # wait for cluster to start
    while cluster_props["ClusterStatus"] != "available":
        print("Still Creating Redshift Cluster")
        time.sleep(15)
        cluster_props = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
        )["Clusters"][0]
        continue

    print("Redshift Cluster is Available")

    global HOST, ARN

    ARN = cluster_props["IamRoles"][0]["IamRoleArn"]

    HOST = cluster_props["Endpoint"]["Address"]

    return redshift, cluster_props


def open_tcp_port(
    ACCESS_KEY_ID: str,
    SECRET_ACCESS_KEY: str,
    REGION: str,
    DWH_PORT: int,
    cluster_props: Any,
) -> None:
    """Function to open ports to allow access to Redshift.

    Args:
        ACCESS_KEY_ID (str): aws access key
        SECRET_ACCESS_KEY (str): aws secret access key
        REGION (str): aws region
        DWH_PORT (int): redshift port # (usually 5439)
        cluster_props (Any): redshift cluster properties object
    """
    # create the ec2 resource
    ec2 = boto3.resource(
        "ec2",
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        region_name=REGION,
    )

    try:
        vpc = ec2.Vpc(id=cluster_props["VpcId"])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT),
        )

    except Exception as e:
        print(e)


def connect_to_cluster(
    HOST: str, DWH_DB: str, DWH_DB_USER: str, DWH_DB_PASSWORD: str, DWH_PORT: int
) -> None:
    """Connects to postgreSQL db and returns conn, cursor objects. Used to test connection.

    Args:
        HOST (str): redshift cluster Endpoint
        DWH_DB (str): database name
        DWH_DB_USER (str): database username
        DWH_DB_PASSWORD (str): database user password
        DWH_PORT (int): database port (usually 5439)
    """
    conn = psycopg2.connect(
        "host={host} dbname={dbname} user={user} password={password} port={port}".format(
            host=HOST,
            dbname=DWH_DB,
            user=DWH_DB_USER,
            password=DWH_DB_PASSWORD,
            port=DWH_PORT,
        )
    )
    cur = conn.cursor()

    print("Successfully Connected to Redshift Cluster")

    return conn, cur


def main():
    """Creates the redshift cluster."""
    roleArn = create_iam_role(
        config.ACCESS_KEY_ID,
        config.SECRET_ACCESS_KEY,
        config.REGION,
        config.DWH_IAM_ROLE_NAME,
    )

    redshift, cluster_props = create_redshift_cluster(
        config.ACCESS_KEY_ID,
        config.SECRET_ACCESS_KEY,
        config.REGION,
        config.DWH_CLUSTER_IDENTIFIER,
        config.DWH_CLUSTER_TYPE,
        config.DWH_NODE_TYPE,
        config.DWH_NUM_NODES,
        config.DWH_DB,
        config.DWH_DB_USER,
        config.DWH_DB_PASSWORD,
        roleArn,
    )

    open_tcp_port(
        config.ACCESS_KEY_ID,
        config.SECRET_ACCESS_KEY,
        config.REGION,
        config.DWH_PORT,
        cluster_props,
    )

    # make sure we can connect to the cluster
    conn, cur = connect_to_cluster(
        HOST, config.DWH_DB, config.DWH_DB_USER, config.DWH_DB_PASSWORD, config.DWH_PORT
    )

    # close the connection when donw
    conn.close()


if __name__ == "__main__":
    main()
