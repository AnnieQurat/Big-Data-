#!/usr/bin/env python
# coding: utf-8

# # 3 - Modes of Income per City (Using RDDs) 

# In[ ]:


# Mode of income of cities is found by:
# Firstly, data is mapped into a (K,v) pair, where the key is city name and value is the income
# Data is partitioned into 4 partitions
# They are then grouped using the city name and the mode of income are is found using mode method from numpy 


# In[1]:


from pyspark import SparkContext
sc =SparkContext()

from scipy import stats
import numpy as numpy


# In[2]:


rdd=sc.textFile('income.txt') #combined incomes in one text document: 'income.txt'
rdd.collect()


# In[5]:


rdd1=rdd.map(lambda x: (x.split(" ")[0],int(x.split(" ")[3])))

rdd2=rdd1.partitionBy(4).persist()

rdd3=rdd2.groupByKey().mapValues(list)

rdd4=rdd3.mapValues(lambda x: numpy.median(x)) #somehow the mode function gives errors.

rdd4.collect()


# In[ ]:




