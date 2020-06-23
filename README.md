# Big-Data

## Contents

1. [Short description](#short-description)
1. [1. Twitter Connectivity Score 1](#twitter-connectivity-score-1)
1. [2. Twitter Connectivity Score 2](#twitter-connectivity-Score-2)
1. [3. Modes of Income per City](#modes-of-income-per-city)

## Short description

### What's the problem?

Using RDD and DataFrame, work with user connectivity on some Twitter data. This requires in-depth knowledge of Big Data tools like Spark and its applications. 

## Twitter Connectivity Score 1 (Using RDD and then DataFrame)

Number of followings and followers for each user is calculated as N and M
The number of times the user appeared in the first column is computed as the number of users the user is following
(k,v) pair is returned having the key as the user, and the value as 1
Reduce function is used to sum the values to get number of followings (N) for each user
Next find the followers of each user, by same step on second column 
Two tables are joined using join action
Values of N and M are multiplied using mapValues transformation

## Twitter Connectivity Score 2 (Using RDD)

The number of followings and followers for each user is calculated here as N and M
The users who are following each other are discarded from the count:
A copy of the data set is taken, and the columns are switched.
The difference between the original data and copied data is found using difference action.
Number of times a user appeared in first column is computed as number of users the user is following. 
By first mapping (k,v) pair 
Reduce function is used to sum the values
Followers of each user is found by same step on second column
Then two tables are joined using join action, values of N and M are multiplied using mapValues transformation

## Modes of Income per City (Using RDDs)

Mode of income of cities is found by:
Firstly, data is mapped into a (K,v) pair, where the key is city name and value is the income
Data is partitioned into 4 partitions
They are then grouped using the city name and the mode of income are is found using mode method from numpy 
