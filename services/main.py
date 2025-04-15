from surveys import surveys_main
from topics import topics_main
from answers import answers_main
from participants import participants_main

import time

def main():
    surveys = surveys_main()
    surveys_ids = surveys['id'].tolist()

    topics = topics_main(surveys_ids)
    answers = answers_main(surveys_ids)
    participants = participants_main()

    return answers


if __name__ == "__main__":
    start = time.time()
    df = main()
    print(f"Execution time: {time.time() - start:.2f} seconds")