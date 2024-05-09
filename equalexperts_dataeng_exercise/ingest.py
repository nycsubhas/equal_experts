import os
import re
import subprocess
import sys
# import argparse
from collections import defaultdict

import duckdb
import pandas as pd

# sys.path.append('equalexperts_dataeng_exercise/db')
# import db
# from equalexperts_dataeng_exercise import db


def ingest(fh):
    src = fh.read()
    counter = 0
    df = defaultdict(list)
    for line in src.split("\n"):
        if line == "":  # A row is a blank line
            # print(f"line No: {counter+1} A blank record!")
            counter += 1
            continue
        if line:  # Line is NOT a blank one
            Id_field = re.findall('"[Ii][Dd]":"[0-9]*"', line)

            Id_field1 = Id_field[0].split(':')
            Id_field = eval(Id_field1[0])
            Id_field_val = eval(eval(Id_field1[-1]))
            # print(f'Id_field: {Id_field}')

            counter += 1
            if counter != Id_field_val:  # One or more Records are missing
                counter = Id_field_val
            # Extraction of the second Field: PostId
            PostId_field1 = re.findall('"[Pp][a-zA-a]*":"[0-9]*"', line)
            PostId_field = eval(PostId_field1[0].split(':')[0])
            PostId_field_val = eval(eval((PostId_field1[0].split(':'))[-1]))
            # print(f'PostId:{PostId_field}  {PostId_field_val}')

            # Extraction of the third Field: VoteTypeId
            VoteTypeId_field1 = re.findall('"[Vv][a-zA-Z]*":"[0-9]*"', line)
            VoteTypeId_field = eval(VoteTypeId_field1[0].split(':')[0])
            # print(f'VoteTypeId_field: {VoteTypeId_field}')
            VoteTypeId_field_val = eval(
                eval((VoteTypeId_field1[0].split(":"))[-1]))
            # print(f'VoteTypeId: {VoteTypeId_field_val}')

            # Extraction of the fourth Field: CreationDate
            parts = re.search(
                '"[Cc][a-zA-Z]*":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}"', line).group()
            parts = parts.partition(':')
            CreationDate_field = eval(parts[0])
            CreationDate_field_val = eval(parts[-1])
            # print(CreationDate_field, CreationDate_field_val)

            # Each column is now a list and the column names are
            # Id_field, PostId_field, VoteTypeId_field
            # and CreationDate_field.
            df[Id_field].append(Id_field_val)
            df[PostId_field].append(PostId_field_val)
            df[VoteTypeId_field].append(VoteTypeId_field_val)
            df[CreationDate_field].append(CreationDate_field_val)
            # input("Enter A Key!")
    return dict(df)


def main():

    # parser = argparse.ArgumentParser()
    # parser.add_argument('fileLocation', nargs='?',default='equalexperts_dataeng_exercise.ingest', type=str, help='Location of the ingest.py relative to root-dir')
    # parser.add_argument('dataLocation', nargs='?',default='uncommitted/votes.jsonl', type=str, help='Location of the votes.jsonl the data file')
    # args = parser.parse_args()
    # path_ingest_file = args.fileLocation
    # print(path_ingest_file)
    #
    # input('fileLocation')
    # assert path_ingest_file == 'equalexperts_dataeng_exercise.ingest', "The path to 'ingest.py' should be correctly specified"

    # path_data_file = args.dataLocation
    # print(path_data_file)
    # assert path_data_file =='uncommitted/votes.jsonl', "The path to 'votes.jsonl' should be correctly specified"

    result = subprocess.run(args=["poetry",
                                  "run",
                                  "exercise",
                                  "tidy",], capture_output=True,)
    result = subprocess.run(args=["poetry",
                                  "run",
                                  "exercise",
                                  "lint",], capture_output=True,)

    if not os.path.exists("votes.jsonl"):  # fetch the data.
        result = subprocess.run(args=["poetry",
                                      "run",
                                      "exercise",
                                      "fetch-data",], capture_output=True,)

        # Create the database by the following method.
    if not os.path.exists("warehouse.db"):
        result = subprocess.run(
            args=[
                "python",
                "-m",
                "equalexperts_dataeng_exercise.db",
                "uncommitted/votes.jsonl",
            ],
            capture_output=True,
        )
        db2 = duckdb.connect("warehouse.db")
        db2.sql("CREATE SCHEMA IF NOT EXISTS blog_analysis")

    # Start Ingestion
        try:
            fh = open('uncommitted/votes.jsonl', 'r', encoding='utf-8')
        except FileNotFoundError as err:
            print(f"Error: {err}")
        finally:
            d = ingest(fh)
            df = pd.DataFrame.from_dict(d)  # df is now a Pandas Data Frame.
            db2.execute(
                "CREATE OR REPLACE TABLE blog_analysis.votes AS SELECT * FROM df")
            db2.sql("SELECT * FROM blog_analysis.votes").show()


# Invoke the main()
if __name__ == "__main__":
    main()
