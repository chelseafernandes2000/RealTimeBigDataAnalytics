Installations and Requirement:
pip install flask
pip install dash
pip install pyspark
pip install psycopg2
pip install kafka-python

You'll also need to download and set up postgresql database, spark, kafka (with zookeeper)

flask_app.py:
Contains flask code for the web app, renders html pages from /templates.
Also, contains kafka producer with writes every new record to kafka topic
You also need to create the kafka topic beforehand

kafkaconsumer.py:
Contains kafkaconsumer program that consumes new records written to kafka topic its subscribed to and
sends that message to pysparkanalytics.py

pysparkanalytics.py:
Here the new record is written to postgresql database using psycopg2. You can do more analytics or cleaning on your data before sending to 
postgresql in here.

dashvisualizations.py:
Here the visualizations fucntions are written with the help of dash. It is set to updates in real time every 1.5 minute
