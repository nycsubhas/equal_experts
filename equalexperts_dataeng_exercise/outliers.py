# Create a view "outlier_weeks"
import os

import duckdb


def main():
    rootdir = os.getcwd()
    # Connect to the database
    database_abspath = os.path.join(rootdir, 'warehouse.db')
    db2 = duckdb.connect(database_abspath)

    # Create a View "outlier_weeks" out of the table "votes"

    db2.sql(''' CREATE OR REPLACE VIEW blog_analysis.outlier_weeks AS
       SELECT YEAR, WeekNumber, VoteCount FROM
       (SELECT YEAR, WeekNumber, VoteCount, ROUND(AVG(VoteCount)
        OVER(),2) AS Average,
        ROUND(ABS(1-VoteCount/Average), 2) as result FROM
       (SELECT DISTINCT year(try_strptime(CreationDate,
       ['%Y-%m-%dT%H:%M:%S.%f', '%m/%d/%Y %H:%M:%S'])) as Year ,
        week(try_strptime(CreationDate,['%Y-%m-%dT%H:%M:%S.%f', 
        '%m/%d/%Y %H:%M:%S'])) % 53 AS WeekNumber,
        count(WeekNumber) OVER(PARTITION BY WeekNumber) AS VoteCount,
        FROM blog_analysis.votes
        ORDER BY WeekNumber))
        WHERE result > 0.2
              ''')
    db2.sql("SELECT * FROM blog_analysis.outlier_weeks").show()
    db2.close()


if __name__ == "__main__":
    main()
