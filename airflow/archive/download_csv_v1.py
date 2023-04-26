from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, date
from zipfile import ZipFile
import wget
import os

today = date.today()

print(today)


#code to extract zip files for years going back to 2018
for y in range(2018,today.year + 1): #can expand to previous years later
   for m in range(1,13):
      url = f"https://s3.amazonaws.com/capitalbikeshare-data/{y}{m:02}-capitalbikeshare-tripdata.zip"
      file_name = f"{y}{m:02}-capitalbikeshare-tripdata" #might need to change later
      
      if not os.path.isfile('/Users/mccli/OneDrive/Documents/GitHub/DEZ-final-project/airflow/data/' + file_name + '.zip'):
         if not (y == today.year and m >= today.month):
            print(f"need to download for year: {y} and month: {m:02}") #delete later
            wget.download(url, out = "/Users/mccli/OneDrive/Documents/GitHub/DEZ-final-project/airflow/data")
            with ZipFile('/Users/mccli/OneDrive/Documents/GitHub/DEZ-final-project/airflow/data/' + file_name + '.zip', mode = "r") as z:
                #with z.open(f"{y}{m:02}-capitalbikeshare-tripdata.csv") as f:
                with z.open(file_name + ".csv") as f:
       
                 # read the dataset
                 train = pd.read_csv(f)
       
                 # display dataset
                 print(train.head())
      else:
         print(f"already have data for year: {y} and month: {m:02}") 


      


#url=f"https://s3.amazonaws.com/capitalbikeshare-data/202201-capitalbikeshare-tripdata.zip"
#wget.download(url, out = "/Users/mccli/OneDrive/Documents/GitHub/DEZ-final-project/airflow/data") #, "/data", might need to change later
#
#with ZipFile(f"202201-capitalbikeshare-tripdata.zip", mode = "r") as z:
#   # open the csv file in the dataset
#   with z.open("202201-capitalbikeshare-tripdata.csv") as f:
#       
#      # read the dataset
#      train = pd.read_csv(f)
#       
#      # display dataset
#      print(train.head())