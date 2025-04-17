from surveys import surveys_main
from topics import topics_main
from answers import answers_main
from participants import participants_main
from factorial.factorial_customfields import head, tlSr, coord
from factorial.factorial_employees import main as employees_main

import time
import asyncio
import pandas as pd

def rename_cols(prefix, df):
    return df.rename(columns={col: prefix + col for col in df.columns})

def main(survey_id):
    surveys = surveys_main(survey_id)
    topics = topics_main(survey_id)
    answers = answers_main(survey_id)
    participants = participants_main()
    heads = asyncio.run(head())
    teamleader = asyncio.run(tlSr())
    coordenador = asyncio.run(coord())
    employees = asyncio.run(employees_main())


    # Adiciona o prefixo em cada df

    surveys = rename_cols("surveys_", surveys)
    topics = rename_cols("topics_", topics)
    answers = rename_cols("answers_", answers)
    participants = rename_cols("participants_", participants)
    employees = rename_cols('employees_', employees)
    

    # Altera o tipo de dado da coluna id pra fazer o merge

    participants['participants_id'] = participants['participants_id'].astype('int64')


    # Tratamento dos custom fields

    employees_cols_to_keep = ['employees_id', 'employees_full_name', 'employees_email', 'employees_manager_id', 'employees_team_leader']
    employees = employees[employees_cols_to_keep]

    heads_cols_to_keep = ['value', 'valuable_id']
    heads = heads[heads_cols_to_keep]
    heads = rename_cols("heads_", heads)
    heads.to_excel('heads.xlsx', index=False)

    teamLeader_cols_to_keep = ['value', 'valuable_id']
    teamleader = teamleader[teamLeader_cols_to_keep]
    teamleader = rename_cols("teamleader_", teamleader)
    teamleader.to_excel('teamLeader.xlsx', index=False)

    coords_cols_to_keep = ['value', 'valuable_id']
    coordenador = coordenador[coords_cols_to_keep]
    coordenador = rename_cols("coordenador_", coordenador)
    coordenador.to_excel('coord.xlsx', index=False)

    df_employees_merge = pd.merge(employees, participants, left_on='employees_email', right_on='participants_email', how='left')
    df_employees_merge = pd.merge(df_employees_merge, teamleader, left_on='employees_id', right_on='teamleader_valuable_id', how='left')
    df_employees_merge = pd.merge(df_employees_merge, coordenador, left_on='employees_id', right_on='coordenador_valuable_id', how='left')
    df_employees_merge = pd.merge(df_employees_merge, heads, left_on='employees_id', right_on='heads_valuable_id', how='left')
    df_employees_merge.to_excel('merge-employees.xlsx', index=False)

    df_merged = pd.merge(answers, surveys, left_on='answers_survey_id', right_on='surveys_id', how='left')
    df_merged = pd.merge(df_merged, topics, left_on='answers_question_id', right_on='topics_question_id', how='left')
    df_merged = pd.merge(df_merged, participants, left_on='answers_reviewee_id', right_on='participants_id', how='left')
    df_merged = pd.merge(df_merged, df_employees_merge, left_on='participants_id', right_on='participants_id', how='left')
    df_merged.to_excel('merge.xlsx', index=False)

    return answers


if __name__ == "__main__":
    survey_id = 102617
    start = time.time()
    df = main(survey_id)
    print(f"Execution time: {time.time() - start:.2f} seconds")
    # df.to_excel("answers.xlsx", index=False)
    




