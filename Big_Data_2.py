#!/usr/bin/env python
# coding: utf-8

# # 2 - Twitter Connectivity Score 2 (Using RDD)

# In[ ]:


# The number of followings and followers for each user is calculated here as N and M
# The users who are following each other are discarded from the count:
# A copy of the data set is taken, and the columns are switched.
# The difference between the original data and copied data is found using difference action.
# Number of times a user appeared in first column is computed as number of users the user is following. 
# By first mapping (k,v) pair 
# Reduce function is used to sum the values
# Followers of each user is found by same step on second column
# Then two tables are joined using join action, values of N and M are multiplied using mapValues transformation


# In[1]:


from pyspark import SparkContext
sc =SparkContext()


# In[2]:


# two copies of the data is imported, the second one has the columns switched.
rdd=sc.textFile("usergraph.txt")
users1=rdd.map(lambda x: (x.split(" ")[0],x.split(" ")[4]))

rdd2=sc.textFile("usergraph.txt")
users2=rdd2.map(lambda x: (x.split(" ")[4],x.split(" ")[0]))

users1.collect()


# In[3]:


# The difference between the two is taken to discard the duplicates (users who are following each other)
users=users1.subtract(users2)
users.collect()


# In[5]:


# mapvalues gives value 0.5 to each key 
# reducebykey is performing the sum on values for each key, to find how many times the user occured
followings=users.mapValues(lambda x: (0.5)).reduceByKey(lambda x,y: x+y)
followings.collect()


# In[6]:


# map creates (k,v) pairs with citynames as keys and 0.5 as values
# reducebykey is performing the sum on values for each key, to find how many times the user occured, which is the values of M for that user (followers)
followers=users.map(lambda x: (x[1],0.5)).reduceByKey(lambda x,y: x+y)
followers.collect()


# In[7]:


# the followings and followers are joind givind a tuple of (N,M) values

Followrs_followings=followings.join(followers)
Followrs_followings.collect()


# In[8]:


# MapValue is used to numtiply the elements of the values tuple (N*M)
# the score of each user is the product of N and M.
N_M_score=Followrs_followings.mapValues(lambda x: (x[0]*x[1]))
N_M_score.collect()


# In[ ]:




