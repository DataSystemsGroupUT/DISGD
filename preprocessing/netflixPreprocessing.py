#!/usr/bin/env python
# coding: utf-8

# # Netflix preprocessing

# In[1]:


import pandas as pd
import time
import datetime
import glob


# In[2]:


#reading dataset from file and put it in the form user,item,rate,timeStamp
empty = pd.DataFrame(columns=["user","item","rate","timestamp"])
path = "part_training_set/mv_*.txt"
for filename in glob.glob(path):
    with open(filename, 'r') as f:
        lines = f.readlines()
        item = int(lines[0].split(":")[0])
    r = pd.read_csv(filename,header = None,skiprows=1)
    r.columns = ["user","rate","timestamp"]
    r["item"] = item
    r = r[["user","item","rate","timestamp"]]
    nf = nf.append(r)


# In[3]:


def convertDateToTimeStamp(date):
    datef = time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple())
    dateInt = int(datef)
    return dateInt


# In[4]:


#convert timestamp from string to int
nf["timeStamp"] = nf["timeStamp"].apply(convertDateToTimeStamp)


# In[5]:


#sort arrcording to timeStamp
nf = nf.sort_values("timeStamp")


# In[6]:


#filter out ratings less than 5
nf_filtered = nf[nf["rate"]==5]


# In[7]:


#save it
nf_filtered.to_csv("nff.txt",header=None,index=None)

