import os
import pandas as pd
import asyncio
import aiohttp

async def fetch(url: str, headers: dict, page: int, session: aiohttp.ClientSession, params: dict = None):
    """
    Busca a página `page` do endpoint `url`.
    Retorna o JSON ou None em caso de erro.
    """
    # Evita mutação de headers compartilhado
    request_headers = headers.copy()
    # Passa página via params em vez de headers
    request_params = params.copy() if params else {}
    request_params['page'] = str(page)

    async with session.get(url, headers=request_headers, params=request_params) as response:
        if response.status == 200:
            return await response.json()
        else:
            print(f"Error fetching page {page}: {response.status}")
            return None

async def fetch_all(url: str, headers: dict, pages: int, session: aiohttp.ClientSession, params: dict = None):
    """
    Dispara requisições para todas as páginas de 1 a `pages` e retorna lista de JSONs.
    """
    tasks = [fetch(url, headers, page, session, params) for page in range(1, pages + 1)]
    return await asyncio.gather(*tasks)

async def get_pages(base_url: str, headers: dict, session: aiohttp.ClientSession, params: dict = None) -> int:
    """
    Obtém número total de páginas a partir do header 'total'.
    """
    async with session.get(base_url, headers=headers, params=params) as response:
        total = int(response.headers.get('total', '0'))
        # Cada página tem até 100 itens
        return total // 100 + (1 if total % 100 else 0)

async def async_main(survey_id: int):
    """
    Executa o fluxo completo para um único survey_id.
    Retorna uma lista de JSONs por página.
    """
    url = f"https://api.qulture.rocks/rest/companies/8378/surveys/{survey_id}/topics"
    params = {
        "include": "questions,question_topic",
        "per_page": "100",
    }
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.getenv('QR_API_KEY')}",
        "user-agent": "Mozilla/5.0",
    }

    async with aiohttp.ClientSession() as session:
        pages = await get_pages(url, headers, session, params)
        data_pages = await fetch_all(url, headers, pages, session, params)
    return data_pages


def main(survey_id: int) -> pd.DataFrame:
    """
    Função principal síncrona que retorna um DataFrame de perguntas para o survey_id.
    """
    data = asyncio.run(async_main(survey_id))

    # Achata apenas a primeira página de tópicos (caso precise de todas, iterar sobre data)
    all_topics = []
    for page_json in data:
        if page_json and 'topics' in page_json:
            all_topics.extend(page_json['topics'])

    # Converte em DataFrame e seleciona colunas
    df = pd.DataFrame(all_topics)[['id', 'name', 'questions']]

    # Explode perguntas em linhas individuais
    new_rows = []
    for _, row in df.iterrows():
        for q in row['questions']:
            new_rows.append({
                'question_id': q['id'],
                'question_name': q['name'],
                'survey_id': survey_id,
                'survey_name': row['name'],
            })
    return pd.DataFrame(new_rows)

if __name__ == "__main__":
    # Exemplo de uso com um único ID
    survey_id = 102617
    df_questions = main(survey_id)

