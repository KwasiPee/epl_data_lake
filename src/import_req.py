import requests

api_key = "yourapikey"
url = "https://api.sportsdata.io/v4/soccer/scores/json/Teams/EPL"
headers = {"Ocp-Apim-Subscription-Key": api_key}

response = requests.get(url, headers=headers)
teams = response.json()

for team in teams:
    print(f"{team['Name']} - ID: {team['TeamId']}")
