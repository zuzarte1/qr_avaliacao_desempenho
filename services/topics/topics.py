import pandas as pd
import requests
import os

url = "https://api.qulture.rocks/rest/companies/8378/surveys/102617/topics?include=questions,question_topic"

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {os.getenv('QR_API_KEY')}"
}

response = requests.get(url, headers=headers)
data = response.json()['topics']
df = pd.DataFrame(data)
topics_cols_to_keep = ['id', 'name', 'questions']
df = df[topics_cols_to_keep]

new_rows = []

for _, row in df.iterrows():
    for topic in row['questions']:
        new_row = {
            'id': topic['id'],
            'name': topic['name'],
            'survey_id': topic['survey_id'],
            'survey_name': row['name'],
        }
        new_rows.append(new_row)

new_df = pd.DataFrame(new_rows)
new_df.to_excel('topicsv2.xlsx', index=False)