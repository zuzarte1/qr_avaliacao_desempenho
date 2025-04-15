import requests
import os
import pandas as pd

import asyncio
import aiohttp

async def fetch(url, headers, page, session):
    headers["page"] = str(page)
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            return await response.json()
        else:
            print(f"Error fetching page {page}: {response.status}")
            return None
    
async def fetch_all(url, headers, pages, session):
    tasks = []
    for page in range(1, pages + 1):
        tasks.append(fetch(url, headers, page, session))
    return await asyncio.gather(*tasks)

async def get_pages(base_url, headers, session):
    async with session.get(f"{base_url}answers", headers=headers) as response:
        headers = dict(response.headers)
        pages = int(headers.get('total', '0')) // 100 + 1
        return pages
    
async def async_main():
    url = "https://api.qulture.rocks/rest/companies/8378/surveys/102617/answers?include=participant%2Creviewer&page=1"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.getenv('QR_API_KEY')}",
        "per_page": "100",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    }
    
    async with aiohttp.ClientSession() as session:
        pages = await get_pages(url, headers, session)
        responses = await fetch_all(url, headers, pages, session)
        data = [item for response in responses if response for item in response['answers']]

    return data


def main():
    data = asyncio.run(async_main())
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