import os
import pandas as pd

import asyncio
import aiohttp

async def fetch(url, headers, page, session, params=None):
    headers["page"] = str(page)
    async with session.get(url, headers=headers, params=params) as response:
        if response.status == 200:
            return await response.json()
        else:
            print(f"Error fetching page {page}: {response.status}")
            return None
    
async def fetch_all(url, headers, pages, session, params=None):
    tasks = []
    for page in range(1, pages + 1):
        tasks.append(fetch(url, headers, page, session, params=params))
    return await asyncio.gather(*tasks)

async def get_pages(base_url, headers, session, params=None):
    async with session.get(f"{base_url}topics", headers=headers, params=params) as response:
        headers = dict(response.headers)
        pages = int(headers.get('total', '0')) // 100 + 1
        return pages
    
async def async_main(surveys_ids):
    url = "https://api.qulture.rocks/rest/companies/8378/surveys/{survey_id}/topics"
    params = {
        "include": "questions,question_topic",
        "per_page": "100",
    }
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.getenv('QR_API_KEY')}",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    }
    
    urls = [url.format(survey_id=survey_id) for survey_id in surveys_ids]

    async with aiohttp.ClientSession() as session:
        urls_pages_tasks = [get_pages(url, headers, session, params) for url in urls]
        urls_pages = await asyncio.gather(*urls_pages_tasks)
        urls_zip = zip(urls, urls_pages)

        fetch_tasks = []
        for url, pages in urls_zip:
            fetch_tasks.append(fetch_all(url, headers, pages, session, params=params))
        data = await asyncio.gather(*fetch_tasks)
    return data

def main(surveys_ids):
    data = asyncio.run(async_main(surveys_ids))

    topics = []
    for item in data:
        first_item = item[0]
        for topic in first_item.get("topics"):
            topics.append(topic)

    df = pd.DataFrame(topics)
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
    return new_df


if __name__ == "__main__":
    surveys_ids = [102617, 101216]
    data = asyncio.run(async_main(surveys_ids))
    df = pd.DataFrame(data)
    df.to_excel("topics.xlsx", index=False)