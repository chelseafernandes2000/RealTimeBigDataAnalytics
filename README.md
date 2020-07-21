# RealTimeBigDataAnalytics
A demonstration of real-time analytics and visualization of E-commerce data using kafka, postgresql, flask and dash

In this project, with the help of kafka and dash I conducted real-time data analysis and visualization on big data downloaded from https://www.kaggle.com/podsyp/how-to-do-product-analytics with size of 1.19 GB

I designed my ecommerce site architecture around the schema of the product dataset. Product dataset is basically about Ads that are shown about products of different categories like clothes, accessories,sneakers etc and it notes the event that is done by the user under the column name "title"

There are basically three types of events:
1) Banner-show
2) Banner-click
3) order


Overall Architecture: <br>
Web app->kafka producer->kafka consumer-> postgresql->dash

![Image of Architecture](https://github.com/chelseafernandes2000/RealTimeBigDataAnalytics/blob/master/architecture.JPG)

In order to show the flow of data from the real site to create real time dashboard, i created a webapp acting as the source of my real-time data. I used flask to create this, as the architecture of this webapp is based of the dataset which in turn is based on three main events I decided the flow like this:


page 1 default-> 
UI with 1 or many ad pics of products of category accessories, clothes,sneakers, sports_nutrition and elesctronics, in the original dataset there is no product as electronic instead there's a product called 'company' which i replaced with 'electronics'.
Now, On this page If user chooses to do nothing (remain still) its counts as banner_show event
else if user clicks on one of the ad
he is diverted to page 2 
Also, at this page anyone random ad will be displayed out of the 5 categories mentioned

page 2->
THis page contains all about the product clicked on as he is redirected from the ad, and a button to order/purchase now
If user does nothing on this page then its banner_click event i.e (user clciked on the ad but didnt order)
else if he clicks on order now button then should go to page 3

page 3->
Just shows order successful, and the event is counted as "order"

As this project is more about analysis, visualization and pipelining data the web app only serves as a demo and a source for getting data for real time analytics

Now whatever event user does his information is sent to kafka producer who pusblishes it to a topic that I created in kafka called "ecommerce"

About Kakfka: <br>
What is Kafka? <br>
Simply put, Kafka is a distributed publish-subscribe messaging system that maintains feeds of messages in partitioned and replicated topics. In the simplest way there are three players in the Kafka ecosystem: producers, topics (run by brokers) and consumers.
* Producers produce messages to a topic of their choice. It is possible to attach a key to each message, in which case the producer guarantees that all messages with the same key will arrive to the same partition.
* Topics are logs that receive data from the producers and store them across their partitions. Producers always write new messages at the end of the log. In our example we can make abstraction of the partitions, since we’re working locally.
* Consumers read the messages of a set of partitions of a topic of their choice at their own pace. If the consumer is part of a consumer group, i.e. a group of consumers subscribed to the same topic, they can commit their offset. This can be important if you want to consume a topic in parallel with different consumers.

I've used kafka as it can instantly publish the new data generated and my kafka consumer will consume it instantly which i can then use to pwrfoem various analytics with valid timestamp and has features like auto_offset_reset=’earliest’: one of the most important arguments. It handles where the consumer restarts reading after breaking down or being turned off and can be set either to earliest or latest. When set to latest, the consumer starts reading at the end of the log. When set to earliest, the consumer starts reading at the latest committed offset. Which is exactly what i want. 

I've used Spark (pyspark.sql) for exploratory analysis on dataset, to find out number of rows, top user, top products etc. The reason of using spark for this is as my dataset is big pandas wouldnt be able to handle it efficiently. Also, spark distributes the data over multiple clusters which helps to manage time very efficiently.

Each record that's been generated through web app will be sent to postgrsql database where the product dataset is already stored. 

Before sending to postgresql database, you can perform more analytics or cleaning using spark. Since my data was already clean i ddidnt do any. 

At the end, I used dash to visualize the analysis done such as live count of orders, banner_click and banner_show for each day that updates itself every minute and refreshes every day at midnight. Also, a real time scatter plot of the numbers of orders over time along with the product categories. And a simple pie-chart of Number of prople using desktop and mobile version for 2019 data.

Ouput Video:
<video width="320" height="240" controls>
  <source src="https://github.com/chelseafernandes2000/RealTimeBigDataAnalytics/blob/master/RealtimeAnalyticsOutput.mp4" type="video/mp4">
</video>



