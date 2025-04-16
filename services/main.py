from surveys import surveys_main
from topics import topics_main
from answers import answers_main
from participants import participants_main

import time

def main(survey_id):
    surveys = surveys_main(survey_id)
    topics = topics_main(survey_id)
    answers = answers_main(survey_id)

    participants = participants_main()

    return answers



if __name__ == "__main__":
    survey_id = 101216
    start = time.time()
    df = main(survey_id)
    print(f"Execution time: {time.time() - start:.2f} seconds")
    df.to_excel("answers.xlsx", index=False)