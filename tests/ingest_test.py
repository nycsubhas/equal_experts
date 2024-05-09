import os
import sys
import subprocess
import duckdb
import pandas as pd
import pytest
import equalexperts_dataeng_exercise.db as d_b


# Your Working Directory should be at 'equal_experts123'
def test_check_root_directory():
    """ Check the current working directory.
    """
    path = os.getcwd()
    curr_dir = path.split('/')[-1]
    assert curr_dir == 'equal_experts123', """PLEASE RUN YOUR PROGRAM FROM \
                                            ROOT-DIRECTORY ENDS WITH equal_expers123 """


# Test Programs to check the ingestion process given in the file ingest.py

def delete_existing_db():
    if os.path.exists("warehouse.db"):
    	os.remove("warehouse.db")
    return

def run_ingestion():
    """
    Run the ingestion process.
    """
    delete_existing_db()
    result = subprocess.run(
        args=[
            "python",
            "-m",
            "equalexperts_dataeng_exercise.ingest",
            "uncommitted/votes.jsonl",
        ],
        capture_output=True,
        )
    #print(result)
    return 

def test_schema_exists():
    """This function checks the existence\
       of a schema blog_analysis"""
       # This method "run_ingestion()" contains at first the deletion of 
       # the database followed by the actual ingestion. 

    run_ingestion()  
    if os.path.exists("warehouse.db"):
        db2 = duckdb.connect("warehouse.db")
        df = db2.sql("""SELECT schema_name
    FROM duckdb_columns()
    WHERE database_name='warehouse'
    limit(1)""").df()
        print(df)
        assert df['schema_name'][0] == 'blog_analysis', "The Schema blog_analysis does not exists"
        db2.close()
        print('ok')
    return
	 

def total_no_fields(fp)->int:
    fp.seek(0)
    return 4 * sum(1 for line in fp)
    
def test_check_fields_for_rows():
    """This function checks the presence of 
    four fields in a line"""

    
    fields = ['{"Id"', "PostId", "VoteTypeId", "CreationDate"] 
    counter = 0; 
    
    with open("uncommitted/votes.jsonl",'r', encoding='utf-8') as fp:
        for line in fp:
            no_fields = 0
            for field in fields:
                if field in line:
                    no_fields += 1
            if no_fields == 4:
                counter += 1 # The no of lines which has four fields. 
        assert total_no_fields(fp) == 4*counter, "Check The Lines in data file"
             
            
def test_check_no_rows_ingested():
    """To Check the number of rows ingested from the File 
       is equal to the number of rows in the database.
    """
    rootdir = os.getcwd()
    abspath = os.path.join(rootdir, 'uncommitted', 'votes.jsonl')
    with open(abspath,encoding='utf-8') as fp:
        no_rows_file = sum(1 for line in fp)
    
    run_ingestion()       
    abspath_database = os.path.join(rootdir, 'warehouse.db')
    db2 = duckdb.connect(abspath_database)
    df = db2.sql('SELECT * FROM blog_analysis.votes').df() 
    db2.close()
    
    assert len(df) == no_rows_file, "Problem in the ingestion process"
    
'''    
# Invoke the __main__
if __name__ == "__main__":
    print('ok...')
    run_ingestion()
    #test_schema_exists()
    #test_check_fields_for_rows()
    #test_check_no_rows_ingested()
'''    
    
    
    
        
        
	

