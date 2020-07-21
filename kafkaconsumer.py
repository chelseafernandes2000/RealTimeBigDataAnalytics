import pandas as pd
import findspark
import pyspark
from pysparkanalytics import *
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameReader
from pyspark.sql.functions import *
import uuid
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from numpy import random
import random

#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing

from json import dumps
from kafka import KafkaProducer

from kafka import KafkaConsumer
from json import loads



consumer = KafkaConsumer(
    'ecommerce',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))


for i in consumer:
    print(i.value)
    title=i.value['title']
    #aabs,abc,ao,cbs,cbc,co,eo,ebs,ebc,sbs,sbc,so,snbc,snbs,sno=sparkmain(i.value,aabs,abc,ao,cbs,cbc,co,eo,ebs,ebc,sbs,sbc,so,snbc,snbs,sno)
    #update(i.value['title'],aabs,abc,ao,cbs,cbc,co,eo,ebs,ebc,sbs,sbc,so,snbc,snbs,sno)
    realsparkmain(i.value)
    '''
    if title=='order':
        ccount,acount,scount,ecount,sncount=orders_by_category(i.value['product'],ccount,acount,scount,ecount,sncount)
   
    ordercount,bccount,bscount=overall(i.value['product'],title,ordercount,bccount,bscount)
    '''
        
   
