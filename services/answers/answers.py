import os
import asyncio
import aiohttp
import pandas as pd
import time

MAX_RETRIES = 5
# Limita até 30 requisições simultâneas (boa prática de controle de concorrência)
SEMAPHORE = asyncio.Semaphore(30)

async def fetch_with_retry(
    url: str,
    headers: dict,
    page: int,
    session: aiohttp.ClientSession,
    params: dict = None
) -> dict:
    """
    Tenta buscar a página `page` no endpoint `url`, com retries exponenciais enquanto o status code não for 200.
    Em caso de 429, respeita o Retry-After ou faz backoff exponencial.
    """
    backoff_base = 2
    task_params = params.copy() if params else {}
    task_params["page"] = str(page)

    for attempt in range(1, MAX_RETRIES + 1):
        async with SEMAPHORE:
            try:
                async with session.get(url, headers=headers.copy(), params=task_params) as response:
                    status = response.status
                    print(f"Fetching page {page}, attempt {attempt}, status {status}...")
                    if status == 200:
                        return await response.json()

                    # Define tempo de espera
                    if status == 429:
                        retry_after = int(response.headers.get("Retry-After", backoff_base ** attempt))
                        print(f"[429] Page {page}, attempt {attempt} — retrying in {retry_after}s...")
                    else:
                        retry_after = backoff_base ** attempt
                        print(f"[{status}] Page {page}, attempt {attempt} — retrying in {retry_after}s...")

                    await asyncio.sleep(retry_after)
            except Exception as e:
                retry_after = backoff_base ** attempt
                print(f"[Exception] Page {page}, attempt {attempt}: {e} — retrying in {retry_after}s...")
                await asyncio.sleep(retry_after)

    print(f"Failed to fetch page {page} after {MAX_RETRIES} attempts.")
    return {}

async def fetch_all(
    url: str,
    headers: dict,
    pages: int,
    session: aiohttp.ClientSession,
    params: dict = None
) -> list:
    """
    Dispara fetch_with_retry para todas as páginas de 1 a `pages`.
    """
    tasks = [fetch_with_retry(url, headers, page, session, params) for page in range(1, pages + 1)]
    return await asyncio.gather(*tasks)

async def get_pages(
    base_url: str,
    headers: dict,
    session: aiohttp.ClientSession,
    params: dict = None
) -> int:
    """
    Busca no header 'total' o número total de itens e calcula quantas páginas existem.
    """
    try:
        async with session.get(f"{base_url}answers", headers=headers.copy(), params=params) as response:
            total_items = int(response.headers.get('total', '0'))
            return (total_items // 100) + (1 if total_items % 100 else 0)
    except Exception as e:
        print(f"[get_pages] Error: {e}")
        return 0

async def async_main(survey_id: int) -> list:
    """
    Fluxo assíncrono para um único survey_id. Retorna lista de JSONs por página.
    """
    url = f"https://api.qulture.rocks/rest/companies/8378/surveys/{survey_id}/answers"
    params = {
        "include": "participant,reviewer",
        "per_page": "100",
    }
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.getenv('QR_API_KEY')}",
        "user-agent": "Mozilla/5.0",
    }

    async with aiohttp.ClientSession() as session:
        pages = await get_pages(url.rsplit("/answers", 1)[0] + "/", headers, session, params)
        if pages <= 0:
            return []
        data_pages = await fetch_all(url, headers, pages, session, params)
    return data_pages


def main(survey_id: int) -> pd.DataFrame:
    """
    Executa o fluxo assíncrono e retorna um DataFrame com todas as respostas.
    """
    start = time.time()
    data = asyncio.run(async_main(survey_id))

    answers = []
    for page in data:
        for answer in page.get("answers", []):
            answers.append(answer)

    if not answers:
        print("Nenhuma resposta encontrada.")
        return pd.DataFrame()

    df = pd.DataFrame(answers)
    cols = ['id', 'grading', 'question_id', 'comment', 'participant_id', 'participant']
    df = df[cols]

    # Extrai campos aninhados
    df['survey_id'] = df['participant'].apply(lambda x: x.get('survey_id'))
    df['survey_participation_id'] = df['participant'].apply(lambda x: x.get('survey_participation_id'))
    df['reviewer'] = df['participant'].apply(lambda x: x.get('reviewer'))
    df['reviewee_id'] = df['participant'].apply(lambda x: x.get('reviewee_id'))
    df['relationship'] = df['participant'].apply(lambda x: x.get('relationship'))
    df['reviewer_id'] = df['reviewer'].apply(lambda x: x.get('id') if x else None)
    df['user'] = df['reviewer'].apply(lambda x: x.get('user') if x else None)
    df['reviewer_name'] = df['user'].apply(lambda x: x.get('name') if x else None)
    df = df.drop(columns=['participant', 'reviewer', 'user'])
    print(df.dtypes)

    print(f"Tempo total: {time.time() - start:.2f} segundos")
    return df

if __name__ == "__main__":
    # Uso para um único ID de survey
    survey_id = 102617
    df = main(survey_id)
    if not df.empty:
        df.to_excel("answers.xlsx", index=False)
        print("Arquivo salvo com sucesso.")
    else:
        print("DataFrame vazio.")
