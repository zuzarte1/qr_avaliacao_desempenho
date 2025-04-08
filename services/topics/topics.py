import requests
import os
import pandas as pd

url = "https://api.qulture.rocks/rest/companies/8378/surveys/102617/topics?include=questions,question_topic"

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {os.getenv('QR_API_KEY')}"
}

response = requests.get(url, headers=headers)
data = response.json()['topics']
df = pd.DataFrame(data)

df.to_excel("topics.xlsx", index=False)