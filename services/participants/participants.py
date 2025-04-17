import requests
from bs4 import BeautifulSoup
import dotenv
import os
import pandas as pd

dotenv.load_dotenv

def main():
    base_url = "https://app.qulture.rocks"
    pages_qurl = f"{base_url}/8378/graphql?op=ActiveContractsCountQuery"
    table_qurl = f"{base_url}/8378/graphql?op=ContractsTableQuery"
    auth_session = get_auth_session(base_url)
    csrf_meta = get_csfr_meta(base_url, auth_session)
    
    pages = get_pages(base_url, pages_qurl, auth_session, csrf_meta)

    all_contracts = []

    for page in range(1, pages + 1):
        contract = get_contracts(base_url, table_qurl, auth_session, csrf_meta, page)
        all_contracts.extend(contract)

    all_contracts = pd.DataFrame(all_contracts)

    all_contracts['image'] = all_contracts['image'].apply(lambda x: x.get('url', ""))

    def extract_supervisor_fields(supervisor):
        if not isinstance(supervisor, dict):
            return pd.Series({
                'supervisor_id': "",
                'supervisor_name': "",
                'supervisor_email': "",
                'supervisor_image': ""
            })
        return pd.Series({
            'supervisor_id': supervisor.get('id', ""),
            'supervisor_name': supervisor.get('name', ""),
            'supervisor_email': supervisor.get('email', ""),
            'supervisor_image': supervisor.get('image', {}).get('url', "")
        })

    all_contracts[['supervisor_id', 'supervisor_name', 'supervisor_email', 'supervisor_image']] = \
        all_contracts['supervisor'].apply(extract_supervisor_fields)
    
    all_contracts.drop(columns=['supervisor'], inplace=True)

    cols_to_keep = ['id', 'name', 'email', 'image', 'supervisor_id', 'supervisor_name', 'supervisor_email', 'supervisor_image']
    all_contracts = all_contracts[cols_to_keep]

    return all_contracts


def get_auth_session(BASE_URL):
    EMAIL = os.getenv("LOGIN")
    PASSWORD = os.getenv('PASSWORD')
    LOGIN_URL   = f"{BASE_URL}/users/sign_in"

    # 1) Cria uma sessão que vai manter cookies (_qulture_session)
    session = requests.Session()

    # 2) GET na página de login → extrai authenticity_token e utf8
    resp = session.get(LOGIN_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    auth_token = soup.find("input", {"name": "authenticity_token"})["value"]
    utf8_val   = soup.find("input", {"name": "utf8"})["value"]

    # 3) POST de login (Rails/Devise retorna JSON com "user" em caso de sucesso)
    login_payload = {
        "utf8": utf8_val,
        "authenticity_token": auth_token,
        "user[email]": EMAIL,
        "user[password]": PASSWORD,
        "commit": soup.find("input", {"type": "submit"})["value"]
    }
    login_headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }
    login_resp = session.post(LOGIN_URL, data=login_payload, headers=login_headers)
    login_resp.raise_for_status()
    login_json = login_resp.json()
    if "user" not in login_json:
        raise RuntimeError(f"Falha no login: {login_json}")
    return session

def get_csfr_meta(BASE_URL, session):
    resp = session.get(BASE_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    csrf_meta = soup.find("meta", {"name": "csrf-token"})["content"]
    return csrf_meta

def get_pages(BASE_URL, PAGES_QURL,session, csrf_meta):
    payload = {
        "query": "query ActiveContractsCountQuery {\n  contracts(filter: {withStatus: ALL}) {\n    pageInfo {\n      totalCount\n      __typename\n    }\n    __typename\n  }\n}\n",
        "variables": {}
    }
    headers = {
    'accept': '*/*',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'origin': BASE_URL,
    'priority': 'u=1, i',
    'referer': f"{BASE_URL}/",
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'track_client_version': '3.164.1297',
    'track_location_href': f"{BASE_URL}/^#/company/8378/contracts",
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'x-csrf-token': csrf_meta
    }

    resp = session.post(PAGES_QURL, json=payload, headers=headers)
    resp.raise_for_status()
    json = resp.json()
    pages = json["data"]["contracts"]["pageInfo"]["totalCount"]//100 + 1

    return pages


def get_contracts(BASE_URL, TABLE_QURL, session, csrf_meta, page):
    payload = {
    "query": """
query ContractsTableQuery($filter: ContractScope, $per: Int, $page: Int) {
contracts(per: $per, page: $page, filter: $filter) {
nodes {
    ...ContractsTableRowFragment
    __typename
}
pageInfo {
    ...PageInfoFragment
    __typename
}
__typename
}
}

fragment ContractsTableRowFragment on Contract {
id
name
email
image(width: 30, height: 30, crop: "fill") {
url
__typename
}
...ContractEntityFragment
supervisor {
id
...SelectContractItemFragment
__typename
}
active
__typename
}

fragment PageInfoFragment on PaginationPageInfo {
nextPage
nodesCount
page
pagesCount
previousPage
totalCount
__typename
}

fragment ContractEntityFragment on Contract {
__typename
id
displayShortName
image(width: 30, height: 30, crop: "fill") {
url
__typename
}
}

fragment SelectContractItemFragment on Contract {
__typename
id
name
nickname
email
image(width: 30, height: 30, crop: "fill") {
url
__typename
}
currentJobPosition {
id
jobTitle {
    id
    name
    abbreviation
    __typename
}
__typename
}
}
""",
    "variables": {
        "per": 100,
        "page": page,
        "filter": {
            "withStatus": "ALL",
            "orderByName": "ASC",
            "active": False
        }
    }
}

    headers = {
    'accept': '/',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'origin': BASE_URL,
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': BASE_URL + '/',
    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'track_client_version': '3.164.1297',
    'track_location_href': BASE_URL + '/^#/company/8378/contracts',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'x-csrf-token': csrf_meta
    }

    resp = session.post(TABLE_QURL, headers=headers, json=payload)
    resp.raise_for_status()
    json = resp.json()
    contracts = json["data"]["contracts"]["nodes"]
    return contracts


if __name__ == "__main__":
    df = main()
    df['id'] = df['id'].astype('int64')

    if not df.empty:
        df.to_excel("participants.xlsx", index=False)
        print("Arquivo salvo com sucesso.")
    else:
        print("DataFrame vazio.")
