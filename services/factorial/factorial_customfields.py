import pandas as pd
import asyncio
import aiohttp
import os

headers = {
    "accept": "application/json",
    "x-api-key": f"{os.environ['FACTORIAL_API_KEY']}"
}

async def fetch_page(session, url, headers, page):
    async with session.get(url, headers=headers, params={'page': page}) as response:
        response.raise_for_status()
        return await response.json()

async def get_data_async(url, headers):
    async with aiohttp.ClientSession() as session:
        # Obter informações iniciais para calcular o total de páginas
        first_response = await fetch_page(session, url, headers, 1)
        total_items = first_response['meta']['total']
        total_pages = (total_items // 100) + 1


        # Adicionar a primeira página aos resultados
        results = [first_response]

        # Criar tarefas para as demais páginas
        tasks = [
            fetch_page(session, url, headers, page)
            for page in range(2, total_pages + 1)
        ]

        # Executar as requisições em paralelo
        results.extend(await asyncio.gather(*tasks))

        # Processar e consolidar os dados
        data = [item for result in results for item in result['data']]

    return pd.DataFrame(data)


async def effective_date():
    url = "https://api.factorialhr.com/api/2024-10-01/resources/custom_fields/values?field_id=3127483"

    employees = await get_data_async(url, headers)

    return employees


async def contract_type():
    url = "https://api.factorialhr.com/api/2024-10-01/resources/custom_fields/values?field_id=2420955"

    employees = await get_data_async(url, headers)

    return employees

async def head():

    url = "https://api.factorialhr.com/api/2024-10-01/resources/custom_fields/values?field_id=3077197"

    employees = await get_data_async(url, headers)

    return employees

async def position():

    url = "https://api.factorialhr.com/api/2024-10-01/resources/custom_fields/values?field_id=3041996"

    employees = await get_data_async(url, headers)

    return employees

async def area():

    url = "https://api.factorialhr.com/api/2024-10-01/resources/custom_fields/values?field_id=4220531"

    employees = await get_data_async(url, headers)

    return employees

async def tlSr():

    url = "https://api.factorialhr.com/api/2024-10-01/resources/custom_fields/values?field_id=6334755"

    employees = await get_data_async(url, headers)

    return employees

async def coord():

    url = "https://api.factorialhr.com/api/2024-10-01/resources/custom_fields/values?field_id=6334757"

    employees = await get_data_async(url, headers)

    return employees

if __name__ == "__main__":
    df = asyncio.run(contract_type())
    cf_cols_to_keep = ['value', 'valuable_id']
    df = df[cf_cols_to_keep]
    df.to_excel('custom_fields.xlsx', index=False)
