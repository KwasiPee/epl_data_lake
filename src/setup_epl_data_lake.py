import boto3
import json
import time
import requests
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# AWS configurations
region = "eu-central-1"
bucket_name = "sarps-epl-analytics-data-lake"
glue_database_name = "sarps-glue-epl-data-lake"
athena_output_location = f"s3://{bucket_name}/athena-results/"

# SportsData.io configurations
api_key = os.getenv("SPORTS_DATA_API_KEY")
epl_endpoint = os.getenv("EPL_ENDPOINT")

# Validate API key before proceeding
if not api_key or not epl_endpoint:
    raise ValueError("ERROR: Missing API key or EPL endpoint. Check your .env file!")

# Create AWS clients
s3_client = boto3.client("s3", region_name=region)
glue_client = boto3.client("glue", region_name=region)
athena_client = boto3.client("athena", region_name=region)

def create_s3_bucket():
    """Create an S3 bucket for storing EPL data if it doesn't already exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"S3 bucket '{bucket_name}' already exists. Skipping creation.")
    except boto3.exceptions.botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':  # Bucket does not exist
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
            print(f"S3 bucket '{bucket_name}' created successfully.")
        else:
            print(f"Error creating S3 bucket: {e}")

def create_glue_database():
    """Create a Glue database for EPL analytics."""
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": glue_database_name,
                "Description": "Glue database for EPL player analytics.",
            }
        )
        print(f"Glue database '{glue_database_name}' created successfully.")
    except Exception as e:
        print(f"Error creating Glue database: {e}")

def fetch_epl_data():
    """Fetch player data for all EPL teams from SportsData.io."""
    try:
        headers = {"Ocp-Apim-Subscription-Key": api_key}
        
        # Get all team IDs from SportsData.io
        teams_response = requests.get(epl_endpoint, headers=headers)
        teams_response.raise_for_status()
        teams = teams_response.json()
        
        all_players = []

        for team in teams:
            team_id = team["TeamId"]
            team_name = team["Name"]
            print(f"Fetching data for {team_name} (ID: {team_id})...")
            
            # Fetch players for each team
            team_endpoint = f"https://api.sportsdata.io/v4/soccer/scores/json/PlayersByTeamBasic/EPL/{team_id}?key={api_key}"
            
            try:
                response = requests.get(team_endpoint, headers=headers)
                response.raise_for_status()
                team_players = response.json()
                
                for player in team_players:
                    player["Team"] = team_name  # ✅ Fix: Ensure correct column name
                    all_players.append(player)
                
                time.sleep(1)  # Avoid API rate limits
                
            except requests.exceptions.HTTPError as http_err:
                if response.status_code == 400:
                    print(f"Skipping {team_name} (ID: {team_id}) due to a 400 error.")
                    continue
                else:
                    print(f"HTTP error fetching data for {team_name}: {http_err}")
                    continue

        print(f"Fetched {len(all_players)} players from all EPL teams.")
        return all_players

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error fetching EPL data: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Error fetching EPL data: {req_err}")
    
    return []


def convert_to_line_delimited_json(data):
    """Convert data to line-delimited JSON format."""
    print("Converting data to line-delimited JSON format...")
    return "\n".join([json.dumps(record) for record in data])

def upload_data_to_s3(data):
    """Upload EPL data to the S3 bucket."""
    try:
        line_delimited_data = convert_to_line_delimited_json(data)
        file_key = "raw-data/epl_player_data.jsonl"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=line_delimited_data
        )
        print(f"Uploaded data to S3: {file_key}")
    except Exception as e:
        print(f"Error uploading data to S3: {e}")

def create_glue_table():
    """Create a Glue table for EPL player data."""
    try:
        glue_client.create_table(
            DatabaseName=glue_database_name,
            TableInput={
                "Name": "epl_players",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "PlayerID", "Type": "int"},
                        {"Name": "FirstName", "Type": "string"},
                        {"Name": "LastName", "Type": "string"},
                        {"Name": "Team", "Type": "string"},  # ✅ Matches raw data now
                        {"Name": "Position", "Type": "string"},
                        {"Name": "Nationality", "Type": "string"},
                        {"Name": "Jersey", "Type": "int"}
                    ],
                    "Location": f"s3://{bucket_name}/raw-data/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"
                    },
                },
                "TableType": "EXTERNAL_TABLE",
            },
        )
        print(f"Glue table 'epl_players' created successfully.")
    except Exception as e:
        print(f"Error creating Glue table: {e}")



def configure_athena():
    """Set up Athena output location."""
    try:
        athena_client.start_query_execution(
            QueryString="CREATE DATABASE IF NOT EXISTS epl_analytics",
            QueryExecutionContext={"Database": glue_database_name},
            ResultConfiguration={"OutputLocation": athena_output_location},
        )
        print("Athena output location configured successfully.")
    except Exception as e:
        print(f"Error configuring Athena: {e}")

def main():
    print("Setting up data lake for EPL player analytics...")
    create_s3_bucket()
    time.sleep(5)
    create_glue_database()
    epl_data = fetch_epl_data()
    if epl_data:
        upload_data_to_s3(epl_data)
    create_glue_table()
    configure_athena()
    print("Data lake setup complete.")

if __name__ == "__main__":
    main()
