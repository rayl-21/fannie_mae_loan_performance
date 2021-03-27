import zipfile
import psycopg2
import os
import csv
from datetime import datetime

def read_db_config(db_config_file):
  """
  Configuration file stores connection parameters for the database.
  """
  configs = {}
  
  with open(db_config_file, 'r') as f:
    lines =  f.readlines()
    for line in lines:
      key_val = line.strip('\n').split('=')
      config_name = key_val[0].strip(' ')
      config_val = key_val[1].strip(' ')
      configs[config_name] = config_val
  
  return configs

def get_col_tuples(col_meta_file):
  """
  The input file stores column names and types for each column in Fannie Mae
  loan performance dataset, with format "column_name,column_type".
  """
  column_tuples_r = []
  column_tuples = []

  with open(col_meta_file, 'r') as c:
    csv_reader = csv.reader(c, delimiter=',')
    for row in csv_reader:
        column_tuples_r.append(row)

  for t in column_tuples_r:
    if t[1] == 'character':
        column_tuples.append((t[0], 'TEXT'))
    if t[1] == 'numeric':
        column_tuples.append((t[0], 'REAL'))
  
  return column_tuples

def create_table_sql(col_tuple_list, table_name):
  """
  Assemble the SQL that creates a table in the database.
  """
  sql = []
  
  for idx, col in enumerate(col_tuple_list):
      if idx != len(col_tuple_list) - 1:
          sql.append(col[0] + ' ' + col[1] + ',')
      else:
          sql.append(col[0] + ' ' + col[1])
  
  sql_str = 'CREATE TABLE {table_name} (\n'.format(table_name=table_name) + '\n'.join(sql) + ');'
  
  return sql_str

if __name__ == '__main__':
  loan_performance_column_file = 'lp_columns.csv' # Path to column data.
  db_config_file = 'config.txt' # Path to database configuration fle.
  table_name = 'loan_performance_test' # Table to be created in the database.
  lp_data_path = 'Performance_All.zip' # Path to loan data.

  db_configs =  read_db_config(db_config_file)
  col_metadata = get_col_tuples(loan_performance_column_file)
  create_sql = create_table_sql(col_metadata, table_name)
  
  conn_str = 'postgresql://{username}@{host}:{port}/{database}'.format(
    username=db_configs['USERNAME'], 
    host=db_configs['HOST'], 
    port=db_configs['PORT'], 
    database=db_configs['DATABASE'])

  # Create a table in PostgreSQL.
  with psycopg2.connect(conn_str) as conn:
    with conn.cursor() as cur:
      cur.execute('DROP TABLE IF EXISTS {table_name}'.format(table_name=table_name))
      cur.execute(create_sql)

  # Ingest data into PostgreSQL database.
  with zipfile.ZipFile(lp_data_path, 'r') as zip_obj:
    lp_data_list = zip_obj.namelist()
    
    for lp in lp_data_list:
      start_extraction = datetime.now()
      zip_obj.extract(lp)
      
      print('Extraction elapsed {}'.format(datetime.now() - start_extraction))
      print('Start ingesting {lp_file}'.format(lp_file=lp))
      start_ingestion = datetime.now()
      with open(lp, 'r') as f:
        with psycopg2.connect(conn_str) as conn:
          with conn.cursor() as cur:
            cur.copy_from(f, table_name, sep='\174', null='') # \174 is octal value for "|".
      
      print('Finished ingesting {lp_file}; {time_elapsed} elapsed.'.format(lp_file=lp, time_elapsed=datetime.now() - start_ingestion))
      
      os.remove(lp)
