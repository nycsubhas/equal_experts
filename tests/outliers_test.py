# Create a view "outlier_weeks"
import os
import subprocess
import pandas as pd
import sys
import pytest
import duckdb


def run_outliers_calculation():
    result = subprocess.run(
        args=["python", "-m", "equalexperts_dataeng_exercise.outliers"],
        capture_output=True,
    )
    result.check_returncode()

def construction_outlier_dataframe_python():
   """ Construction of outlier dataframe
       using python pandas.
   """
   run_outliers_calculation()
   rootdir = os.getcwd()
    # Connect to the database
   database_abspath = os.path.join(rootdir, 'warehouse.db')
   db = duckdb.connect(database_abspath)
   #db.sql("SELECT * FROM blog_analysis.votes").show()
   # Fetch the data from database table votes.
   df = db.sql("SELECT * FROM blog_analysis.votes").fetchdf()

    
   df['CreationDate'] = pd.to_datetime(df['CreationDate'],errors ='coerce')
   df['WeekNumber'] = df['CreationDate'].apply(lambda x: x.weekofyear)
   df['Year'] = df['CreationDate'].apply(lambda x: x.year)
   
   # Number of Votes casted per Week. 'VoteCount' column. 
   df['VoteCount'] = df.groupby(['WeekNumber']).transform('size')
   
   df1 = df[['Year','WeekNumber', 'VoteCount']]
   df2 = df1.drop_duplicates(['Year', 'WeekNumber', 'VoteCount'])
   # Result containing the outliers. 
   avg = df2['VoteCount'].mean()
   lst = list(round(abs(1 - df2['VoteCount']/avg), 2))
   
   # Create a column called "result"     
   df2=df2.assign(result=lst)
   # Apply outlier condition to select the records 
   # which has outliers.
   df2 = df2[df2['result'] > 0.2] 
   df2=df2[['Year', 'WeekNumber', 'VoteCount']]
   
   # get the outlier dataframe from using pandas dataframe.
   df2 = df2.sort_values(by=['WeekNumber','Year'])
   return df2, db

def test_check_size_outlier_dataframe():   
   """ Get the outlier data frame using pandas.
       and compare its size with that of the 
       outlier dataframe using view from the database 
       and this dataframe is constructed using 
       one line SQL commands.
   """
   df, db = construction_outlier_dataframe_python() # Outlier Dataframe from python. 
   df_sql = db.sql("SELECT * FROM blog_analysis.outlier_weeks").fetchdf() # Outlier Dataframe from SQL
   db.close()

   # Test the two dataframes df2 and df_sql consists of equal number of rows.
   assert len(df_sql) == len(df), "Error in the calculation of outliers"

def main():
   test_check_size_outlier_dataframe()
   
   
if __name__ == "__main__":
   main()


