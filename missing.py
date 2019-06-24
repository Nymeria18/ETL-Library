#!/usr/bin/env python
# coding: utf-8

# In[98]:


from pyspark.sql import SQLContext
import pyspark.sql.functions
from pyspark.sql.functions import avg,col


# In[77]:


sqlContext=SQLContext(sc)
form=raw_input("Select the input format\n1.csv\n2.json\n3.jbdc\n4.orc\n5.paraquet\n6.libsvm\n7.text")
form=str(form)
path=raw_input("Enter the path")
path=str(path)
df=sqlContext.read.format(form).option("header","True").option("inferSchema", "True").load(path)


# In[78]:


df.show()


# In[79]:


df.dtypes


# In[83]:


df.describe().show()


# In[84]:


print("No. of rows in the dataframe="+str(df.count()))
print("No. of columns in the dataframe="+str(len(df.columns)))


# In[120]:


Nulls={}
for x in range(len(df.columns)):
    dfNoNulls=df.na.drop(how="all",subset=df.columns[x])
    Nulls[df.columns[x]]=((df.count()-dfNoNulls.count()))


# In[121]:


print Nulls


# In[124]:


threshd=input("Threshold value of missing rows to drop column?")
for x in Nulls:
    if Nulls[x]>=threshd:
        print("dropping "+x)
        df=df.drop(x)


# In[125]:


print("Imputing Mean value in the rest of the missing value columns")
nonnull=df.na.drop(how="all")
for x in Nulls:
    if Nulls[x]>=0 and Nulls[x]<=threshd:
       meanValue=nonnull.agg(avg(col(x))).first()[0]
       print(x, meanValue)
       if type(meanValue) is float:
           df=df.na.fill(meanValue, [x])


# In[ ]:




