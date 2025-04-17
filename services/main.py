from surveys import surveys_main
from topics import topics_main
from answers import answers_main
from participants import participants_main
from factorial.factorial_customfields import head, tlSr, coord

import time
import asyncio

def main(survey_id):
    surveys = surveys_main(survey_id)
    topics = topics_main(survey_id)
    answers = answers_main(survey_id)

    participants = participants_main()

    heads_cols_to_keep = ['value', 'valuable_id']
    heads = heads[heads_cols_to_keep]
    heads.to_excel('heads.xlsx', index=False)

    teamLeader_cols_to_keep = ['value', 'valuable_id']
    teamleader = teamleader[teamLeader_cols_to_keep]
    teamleader.to_excel('teamLeader.xlsx', index=False)

    coords_cols_to_keep = ['value', 'valuable_id']
    coordenador = coordenador[coords_cols_to_keep]
    coordenador.to_excel('coord.xlsx', index=False)

    

    return answers

heads = asyncio.run(head())
teamleader = asyncio.run(tlSr())
coordenador = asyncio.run(coord())


if __name__ == "__main__":
    survey_id = 101216
    start = time.time()
    df = main(survey_id)
    print(f"Execution time: {time.time() - start:.2f} seconds")
    df.to_excel("answers.xlsx", index=False)




