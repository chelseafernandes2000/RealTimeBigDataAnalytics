import pandas as pd
import findspark
import psycopg2 
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import datetime
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameReader
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()


username='postgres' 
password='123'

con = psycopg2.connect(database='ecommerce',user=username,password=password);
cursor = con.cursor()


def realsparkmain(record):
    order_id=record["order_id"]
    user_id=record["user_id"]
    page_id=record["page_id"]
    product=record["product"]
    site_version=record["site_version"]
    time=record["time"]
    title=record["title"]
    target=record["target"]
    date=record["date"]
    insertquery = "insert into ecommerce values ('"+str(order_id)+"','"+str(user_id)+"','"+str(page_id)+"','"+str(product)+"','"+str(site_version)+"','"+str(time)+"','"+str(title)+"',"+str(target)+",'"+str(date)+"');"
    cursor.execute(insertquery)
    con.commit()
    print("New Record Inserted in database")
