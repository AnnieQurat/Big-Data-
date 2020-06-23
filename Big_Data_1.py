#!/usr/bin/env python
# coding: utf-8

# # 1 - Twitter Connectivity Score 1 (Using RDD and then DataFrame)

# In[ ]:


# Big Data Analysis 

# Number of followings and followers for each user is calculated as N and M
# The number of times the user appeared in the first column is computed as the number of users the user is following
# (k,v) pair is returned having the key as the user, and the value as 1
# Reduce function is used to sum the values to get number of followings (N) for each user
# Next find the followers of each user, by same step on second column 
# Two tables are joined using join action
# Values of N and M are multiplied using mapValues transformation


# In[1]:


from pyspark.sql import *
from pyspark import SparkContext
sc =SparkContext()
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

spark = sqlContext.sparkSession
spark


# ## Part A - Using RDD 

# In[2]:


# importing dataset as textfile
rdd=sc.textFile("usergraph.txt") #combined text file imported
rdd.collect()


# In[3]:


# create rdd by splitting lines of dataset using first column only
# use map to create (k,v) pairs 
users1=rdd.map(lambda x: (x.split(" ")[0],1))
# users1.getNumPartitions()
users1_p=users1.partitionBy(3).persist()
# reducebykey is performing the sum on values for each key, to find how many times the user occured, which is the values of N for that user (followings)
Followings=users1_p.reduceByKey(lambda x,y:x+y)
Followings.collect()


# In[4]:


# create rdd by splitting the lines of dataset using data in second column
# map creates (k,v) pairs with citynames as keys and 1 as values
users2=rdd.map(lambda x: (x.split(" ")[4], 1))
# users2.getNumPartitions()
users2_p=users2.partitionBy(3).persist()
# reducebykey is performing the sum on values for each key, to find how many times the cityname occured, which is the values of M for that city (followers)
Followers=users2_p.reduceByKey(lambda x,y: x+y)
Followers.collect()


# In[5]:


# the followings and followers are joind givind a tuple of (N,M) values
score=Followings.join(Followers)
score.collect()


# In[6]:


# MapValue is used to numtiply the elements of the values tuple (N*M)
# the score of each user is the product of N and M.
finalscore=score.mapValues(lambda x: (x[0]*x[1]))
finalscore.collect()


# ## Part B - Using DataFrame

# In[7]:


import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
import collections


# In[18]:


df = sc.textFile("usergraph.txt")


# In[19]:


df = df.map(lambda line: line.split(" ")) #rdd is the textFile of usergraphs imported above


# In[20]:


df = spark.read.csv(df)
df.show()


# In[21]:


df2=df.groupBy('_c0')
df2.count().show()


# In[23]:


df3=df.groupBy('_c4')
df3.count().show()


# In[ ]:




