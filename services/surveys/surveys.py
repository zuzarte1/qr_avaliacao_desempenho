import requests
import os
import pandas as pd

url = "https://api.qulture.rocks/rest/companies/8378/surveys"

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {os.getenv('QR_API_KEY')}"
}

response = requests.get(url, headers=headers)
data = response.json()['surveys']
df = pd.DataFrame(data)

surveys_cols_to_keep = ['id', 'name', 'stage', 'settings', 'participants_count', 'draft']
df = df[surveys_cols_to_keep]

df['start_at'] = df['settings'].apply(lambda x: x.get('answer_period').get('start_at'))
df['end_at'] = df['settings'].apply(lambda x: x.get('answer_period').get('end_at'))
df['grades_amount'] = df['settings'].apply(lambda x: x.get('grades_amount'))

df = df.drop('settings', axis=1)

df['start_at'] = pd.to_datetime(df['start_at'])
df['end_at'] = pd.to_datetime(df['end_at'])


df['start_at'] = df['start_at'].dt.tz_localize(None)
df['end_at'] = df['end_at'].dt.tz_localize(None)

df.to_excel("surveys.xlsx", index=False)