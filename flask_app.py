from flask import *
from flask_sqlalchemy import *
import time
import threading
import random
import ntpath
import re
import uuid
import datetime
import pandas as pd
import findspark
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameReader
from pyspark.sql.functions import *
import uuid
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing

from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))



user_id_list=['6e1d9fca53a19d4f105947348fd1d627', 
    '27af71559f2723868c078c49bd3ca0db',
    '2b9a39dddd5b7a18891dfd0d3aa3c599',
    'b89dfb3b9656f8836849bcb874afac70',
    'aa33d045826d234eb5858674dbfe93f2'] #list of top 5 users
i=random.randint(0, 4)
user_id=user_id_list[i]

j=random.randint(0, 1)
site=['desktop','mobile']
site_version=site[j]



app = Flask(__name__)
@app.route('/')
def home():
    home_page_id='a3d2de7675556553a5f08e4c88d2c228' #1st most common id has been taken as home page id for now
    unique_order_id=uuid.uuid4().hex[:32] #unique_id
    k=random.randint(0, 4)
    l=['static/images/accessories.jpg','static/images/electronics.jpg','static/images/sneakers.jpg','static/images/sports_nutrition.jpg','static/images/clothes.jpg']
    img1=l[k]
    product_name=ntpath.basename(img1)

    product=product_name.replace('.jpg','')

    target_home=0
    title='banner_show'

    now = datetime.datetime.now()
    timee=now.strftime("%m/%d/%Y %H:%M:%S")
    date=now.strftime("%m/%d/%Y")
    print(unique_order_id,user_id,home_page_id,product,site_version,timee,title,target_home,date)

    data = {'order_id':unique_order_id,'user_id':user_id,'page_id':home_page_id,'product':product,'site_version':site_version,'time':timee,'title':title,'target':target_home,'date':date}
    producer.send('ecommerce', value=data)

    return render_template('index.html',img1=img1,product=product,k=k)



@app.route('/product',methods=['GET'])
def product_page():
    unique_order_id=uuid.uuid4().hex[:32] #unique_id
    product_page_id='f218ebcda49e5da7ac8b4e51aed47550' #2nd most common page id
    title="banner_click"
    k=int(request.args.get('my_var', None))
    l=['static/images/accessories.jpg','static/images/electronics.jpg','static/images/sneakers.jpg','static/images/sports_nutrition.jpg','static/images/clothes.jpg']
    img1=l[k]
    product_name=ntpath.basename(img1)

    product=product_name.replace('.jpg','')
    target=0
    now = datetime.datetime.now()
    timee=now.strftime("%m/%d/%Y %H:%M:%S")

    date=now.strftime("%m/%d/%Y")
    print(unique_order_id,user_id,product_page_id,product,site_version,timee,title,target,date)

    data = {'order_id':unique_order_id,'user_id':user_id,'page_id':product_page_id,'product':product,'site_version':site_version,'time':timee,'title':title,'target':target,'date':date}
    producer.send('ecommerce', value=data)

    return render_template('product.html',img1=img1,k=k,product=product)

@app.route('/order',methods=['GET'])
def order_page():
    unique_order_id=uuid.uuid4().hex[:32] #unique_id
    order_page_id='e6d8545daa42d5ced125a4bf747b3688' #3rd most common page id
    title="order"
    k=int(request.args.get('my_var', None))
    l=['static/images/accessories.jpg','static/images/electronics.jpg','static/images/sneakers.jpg','static/images/sports_nutrition.jpg','static/images/clothes.jpg']
    img1=l[k]
    product_name=ntpath.basename(img1)

    product=product_name.replace('.jpg','')

    target=1
    now = datetime.datetime.now()
    timee=now.strftime("%m/%d/%Y %H:%M:%S")
    date=now.strftime("%m/%d/%Y")
    print(unique_order_id,user_id,order_page_id,product,site_version,timee,title,target,date)

    data = {'order_id':unique_order_id,'user_id':user_id,'page_id':order_page_id,'product':product,'site_version':site_version,'time':timee,'title':title,'target':target,'date':date}
    producer.send('ecommerce', value=data)

    return render_template('order.html',img1=img1,k=k,product=product)

if __name__ == '__main__':
    app.debug = True
    
    app.run()