import os
import asyncio
import aiohttp
import pandas as pd
import time

MAX_RETRIES = 5
SEMAPHORE = asyncio.Semaphore(30)  # Limita 5 conexões simultâneas

async def fetch_with_retry(url, headers, page, session, params=None):
    backoff_base = 2
    task_params = params.copy() if params else {}
    task_params["page"] = str(page)

    for attempt in range(1, MAX_RETRIES + 1):
        async with SEMAPHORE:
            try:
                async with session.get(url, headers=headers.copy(), params=task_params) as response:
                    print(f"Fetching page {page}, attempt {attempt}...")
                    print(f"Response status: {response.status}")
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", backoff_base ** attempt))
                        print(f"[429] Page {page}, attempt {attempt} — retrying in {retry_after}s...")
                        await asyncio.sleep(retry_after)
                    else:
                        print(f"[{response.status}] Error fetching page {page}")
                        return {}
            except Exception as e:
                print(f"[Exception] Page {page}, attempt {attempt}: {e}")
                await asyncio.sleep(backoff_base ** attempt)

    print(f"Failed to fetch page {page} after {MAX_RETRIES} attempts.")
    return {}

async def fetch_all(url, headers, pages, session, params=None):
    tasks = [fetch_with_retry(url, headers, page, session, params=params) for page in range(1, pages + 1)]
    return await asyncio.gather(*tasks)

async def get_pages(base_url, headers, session, params=None):
    try:
        async with session.get(f"{base_url}answers", headers=headers.copy(), params=params) as response:
            headers_resp = dict(response.headers)
            total_items = int(headers_resp.get('total', '0'))
            return (total_items // 100) + (1 if total_items % 100 else 0)
    except Exception as e:
        print(f"[get_pages] Error: {e}")
        return 0

async def async_main(surveys_ids):
    url_template = "https://api.qulture.rocks/rest/companies/8378/surveys/{survey_id}/answers"
    params = {
        "include": "participant,reviewer",
        "per_page": "100",
    }
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.getenv('QR_API_KEY')}",
        "user-agent": "Mozilla/5.0",
    }

    urls = [url_template.format(survey_id=survey_id) for survey_id in surveys_ids]

    async with aiohttp.ClientSession() as session:
        pages_per_url = await asyncio.gather(*[
            get_pages(url.rsplit("/answers", 1)[0] + "/", headers, session, params) for url in urls
        ])

        fetch_tasks = [
            fetch_all(url, headers, pages, session, params=params)
            for url, pages in zip(urls, pages_per_url) if pages > 0
        ]
        data = await asyncio.gather(*fetch_tasks)
    return data

def main(surveys_ids):
    data = asyncio.run(async_main(surveys_ids))

    answers = []
    for item in data:
        if not item:
            continue
        for page in item:
            for answer in page.get("answers", []):
                answers.append(answer)

    if not answers:
        print("Nenhuma resposta encontrada.")
        return pd.DataFrame()

    df = pd.DataFrame(answers)
    answers_cols_to_keep = ['id', 'grading', 'question_id', 'comment', 'participant_id', 'participant']
    df = df[answers_cols_to_keep]

    df['survey_id'] = df['participant'].apply(lambda x: x.get('survey_id'))
    df['survey_participation_id'] = df['participant'].apply(lambda x: x.get('survey_participation_id'))
    df['reviewer'] = df['participant'].apply(lambda x: x.get('reviewer'))
    df['reviewee_id'] = df['participant'].apply(lambda x: x.get('reviewee_id'))
    df['relationship'] = df['participant'].apply(lambda x: x.get('relationship'))
    df['reviewer_id'] = df['reviewer'].apply(lambda x: x.get('id') if x else None)
    df['user'] = df['reviewer'].apply(lambda x: x.get('user') if x else None)
    df['reviewer_name'] = df['user'].apply(lambda x: x.get('name') if x else None)

    df = df.drop(columns=['participant', 'reviewer', 'user'])

    return df

if __name__ == "__main__":
    start = time.time()
    surveys_ids = [102617, 101216]
    df = main(surveys_ids)
    print(f"Tempo total: {time.time() - start:.2f} segundos")
    if not df.empty:
        df.to_excel("answers.xlsx", index=False)
        print("Arquivo salvo com sucesso.")
    else:
        print("DataFrame vazio.")
