# Data-analytics-using-Hadoop-MapReduce-and-Spark
1.Generated various statistics using Yelp Data set these included filtering ,getting Top k.
2.Efficiently implemented map side and reduce side join.
3.Generated same statistics using Spark as the underlying framework and measured the time difference.
4.Compared the performance of broadcast variables vs RDD.
5.Technologies:Spark, Hadoop,Map-Reduce


Dataset Description.
The dataset comprises of three csv files, namely user.csv, business.csv and review.csv.  

Business.csv file contain basic information about local businesses. 
Business.csv file contains the following columns "business_id","full_address","categories"

'business_id': (a unique identifier for the business)
'full_address': (localized address), 
'categories': [(localized category names)]  

review.csv file contains the star rating given by a user to a business. Use user_id to associate this review with others by the same user. Use business_id to associate this review with others of the same business. 

review.csv file contains the following columns "review_id","user_id","business_id","stars"
 'review_id': (a unique identifier for the review)
 'user_id': (the identifier of the reviewed business), 
 'business_id': (the identifier of the authoring user), 
 'stars': (star rating, integer 1-5),the rating given by the user to a business

user.csv file contains aggregate information about a single user across all of Yelp
user.csv file contains the following columns "user_id","name","url"
user_id': (unique user identifier), 
'name': (first name, last initial, like 'Matt J.'), this column has been made anonymous to preserve privacy 
'url': url of the user on yelp


After being familiar with the data - you are required to write efficient Hadoop Map-

Reduce programs in Java to find the following information ::

Hadoop Exercise

Q1. 
 List each business Id that are located in “Palo Alto” using the full_address column as the filter column. 

Sample output:

23244444
232ewe33

Q2 

Find the top ten rated businesses using the average ratings. 
Recall that star column in review.csv file represents the rating.

Please answer the question by calculating the average ratings given to each business using the review.csv file. 

Sample output:
business id              
xdf12344444444


Q3:
List the  business_id , full address and categories of the Top 10 businesses using the average ratings.  

This will require you to use  review.csv and business.csv files.

Please use reduce side join and job chaining technique to answer this problem.


Sample output:
business id               full address           categories                                    avg rating
xdf12344444444,              CA 91711         List['Local Services', 'Carpet Cleaning']	         5.0

Q4: 
List the 'user id' and 'stars' of users that reviewed businesses located in Stanford 
Required files are 'business'  and 'review'.

Please use Map side join technique to answer this problem.
Hint: Please load all data in business.csv file into the distributed cache. 

Sample output
                                                   
	       
User id                      stars
0WaCdhr3aXb0G0niwTMGTg       4.0

Spark Exercise 

Q1. Given input address (any part of the address e.g., city or state), find all
the business ids located at the address. You must take the input address in the
command line. [For example, if the input address is Stanford then you need to
find all businesses with stanford in the address column] [You only need
business.csv file to get the answer.]

Q2.
a. Start spark-shell in local mode using all the processor cores on your
system or the cluster. (Very important)List the business_id of the Top 10 businesses using the average ratings. This
will require you to use review.csv. Please answer the question by calculating the
average ratings given to each business using the review.csv file. Next, sort the
output based on the business_id before taking the top 10 businesses using the
average ratings.
b. Rerun Q2a using Yarn mode. Please solve using our cs6360 cluster. This
questions shows how spark can be used on multiple systems in a cluster.
Load all the dataset to hadoop cluster as you did in homework1.
Use the address of the file on the cluster as input to your scala script.
Start spark-shell in YARN mode using Cs6360 spark cluster.
This spark cluster consist 6 hadoop machine nodes. Using the following
parameters Rerun your scala script from question 2a.
Set executor memory =2G
executor cores = 6.
num-executors = 6
For example, the command is as follows.
spark-shell --master yarn-client --executor-memory 4G --executor-cores 7 --num-
executors 6
Please measure the execution time of your program using the local mode
(Q2a )and yarn mode (Q2b). Please compare the measured time.
Note: Spark supports only scala or java in YARN mode.

Q3a:
List the business_id , full address and categories of the Top 10
businesses using the average ratings.
Use the files business.csv and review.csv.
Please sort the output based on the business_id before taking the top 10 business
using the average ratings.

Q3b:
Using broadcast variable in spark to store the business data, re implement
your solution to question 3a.
Please measure the execution time of your program when using spark RDD
(Q3a )and when using Broadcast variable (Q3b). Please compare the
measured time.

