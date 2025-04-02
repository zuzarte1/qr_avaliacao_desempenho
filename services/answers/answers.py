import requests
import os
import pandas as pd

url = "https://api.qulture.rocks/rest/companies/8378/surveys/102617/answers?include=participant%2Creviewer&per_page=20&page=1"
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {os.getenv("QR_API_KEY")}"
}

response = requests.get(url, headers=headers)
data = response.json()['answers']
df = pd.DataFrame(data)
print(df)
df.to_excel("questions.xlsx", index=False)