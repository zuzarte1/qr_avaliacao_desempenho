import requests
import pandas as pd

url = "https://api.qulture.rocks/rest/companies/8378/surveys/102617/participants"

headers = {
    "accept": "application/json",
    "Authorization": "Bearer 88b3bc39733718bd02a7c486e957a74d837565ed24ca611e23542c65cedfb06b"
}

response = requests.get(url, headers=headers)

response = response.json()['participants']

df = pd.DataFrame(response)
df.to_excel('output.xlsx', index=False)