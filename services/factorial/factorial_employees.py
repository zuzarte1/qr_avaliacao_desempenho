import pandas as pd
import asyncio
import aiohttp
import os

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

async def main():
    url = "https://api.factorialhr.com/api/2024-10-01/resources/employees/employees?only_active=false&only_managers=false"
    headers = {
        "accept": "application/json",
        "x-api-key": f"{os.environ['FACTORIAL_API_KEY']}"
    }

    # Obter dados da API
    employees = await get_data_async(url, headers)

    employees['team_leader'] = pd.merge(employees, employees, left_on="manager_id", right_on="id", how="left")["email_y"]

    return employees

if __name__ == "__main__":
    df = asyncio.run(main())
    employees_cols_to_keep = ['id', 'full_name', 'email', 'manager_id', 'team_leader']
    df = df[employees_cols_to_keep]
    df.to_excel('factorial_employees.xlsx', index=False)
