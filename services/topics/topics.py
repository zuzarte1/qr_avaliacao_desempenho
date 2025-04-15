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
    async with session.get(f"{base_url}topics", headers=headers) as response:
        headers = dict(response.headers)
        pages = int(headers.get('total', '0')) // 100 + 1
        return pages
    
async def async_main():
    url = "https://api.qulture.rocks/rest/companies/8378/surveys/102617/topics?include=questions,question_topic"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.getenv('QR_API_KEY')}",
        "per_page": "100",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    }
    
    async with aiohttp.ClientSession() as session:
        pages = await get_pages(url, headers, session)
        responses = await fetch_all(url, headers, pages, session)
        data = [item for response in responses if response for item in response['topics']]

    return data

def main():
    data = asyncio.run(async_main())
    df = pd.DataFrame(data)
    topics_cols_to_keep = ['id', 'name', 'questions']
    df = df[topics_cols_to_keep]

    new_rows = []

    for _, row in df.iterrows():
        for topic in row['questions']:
            new_row = {
                'question_id': topic['id'],
                'question_name': topic['name'],
                'survey_id': topic['survey_id'],
                'survey_name': row['name'],
            }
            new_rows.append(new_row)

    new_df = pd.DataFrame(new_rows)
    new_df.to_excel('topics.xlsx', index=False)