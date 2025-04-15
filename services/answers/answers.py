import requests
import os
import pandas as pd

url = "https://api.qulture.rocks/rest/companies/8378/surveys/102617/answers?include=participant%2Creviewer&per_page=20&page=1"

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {os.getenv('QR_API_KEY')}"
}

response = requests.get(url, headers=headers)
data = response.json()['answers']
df = pd.DataFrame(data)

answers_cols_to_keep = ['id', 'grading', 'question_id', 'comment', 'participant_id', 'participant']
df = df[answers_cols_to_keep]

df['survey_id'] = df['participant'].apply(lambda x: x.get('survey_id'))
df['survey_participation_id'] = df['participant'].apply(lambda x: x.get('survey_participation_id'))
df['reviewer'] = df['participant'].apply(lambda x: x.get('reviewer'))
df['reviewee_id'] = df['participant'].apply(lambda x: x.get('reviewee_id'))
df['relationship'] = df['participant'].apply(lambda x: x.get('relationship'))
df['reviewer_id'] = df['reviewer'].apply(lambda x: x.get('id'))
df['user'] = df['reviewer'].apply(lambda x: x.get('user'))
df['reviewer_name'] = df['user'].apply(lambda x: x.get('name'))

df = df.drop('participant', axis=1)
df = df.drop('reviewer', axis=1)
df = df.drop('user', axis=1)




df.to_excel("answers.xlsx", index=False)