#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SQLContext
import pyspark.sql.functions
from pyspark.sql.functions import avg,col


# In[2]:


sqlContext=SQLContext(sc)
form=raw_input("Select the input format\n1.csv\n2.json\n3.jbdc\n4.orc\n5.paraquet\n6.libsvm\n7.text")
form=str(form)
path=raw_input("Enter the path")
path=str(path)
df=sqlContext.read.format(form).option("header","True").option("inferSchema", "True").load(path)


# In[ ]:


df.show()


# In[3]:


df.dtypes


# In[ ]:


df.describe().show()


# In[4]:


print("No. of rows in the dataframe="+str(df.count()))
print("No. of columns in the dataframe="+str(len(df.columns)))


# In[5]:


Nulls={}
for x in range(len(df.columns)):
            dfNoNulls=df.na.drop(how="all",subset=df.columns[x])
            Nulls[df.columns[x]]=((df.count()-dfNoNulls.count()))


# In[6]:


print Nulls


# In[7]:


threshd=input("Threshold value of missing rows to drop column?")
for x in Nulls:
    if Nulls[x]>=threshd:
        print("dropping "+x)
        df=df.drop(x)


# In[8]:


print("Imputing Mean value in the rest of the missing value numeric columns")
nonnull=df.na.drop(how="all")


# In[9]:


for x in Nulls:
        if Nulls[x]>0 and Nulls[x]<=threshd:
           meanValue=nonnull.agg(avg(col(x))).first()[0]
           print(x, meanValue)
           if type(meanValue) is float:
               df=df.na.fill(meanValue, [x])


# In[10]:


Nulls={}
for x in range(len(df.columns)):
            dfNoNulls=df.na.drop(how="all",subset=df.columns[x])
            Nulls[df.columns[x]]=((df.count()-dfNoNulls.count()))


# In[12]:


print("Dropping missing value rows of non numeric columns")
for x in Nulls:
    if Nulls[x]>0:
        df=df.na.drop(subset=[x])


# In[20]:


intp=[]
for x,y in df.dtypes:
    if y=='int':
        intp.append(x)
print intp


# In[30]:


from pyspark.mllib.util import MLUtils
from pyspark.ml.feature import Normalizer

Normopt=raw_input("Performing nomalization\nSelect type:\n1.$L^1$ norm\n2.$L^\infty$")

if Normopt is "$L^1$":
    for x in intp:
        normalizer = Normalizer(inputCol=x, outputCol=x, p=1.0)
        l1NormData = normalizer.transform(df)
elif Normopt is "$L^\infty$":
    for x in intp:
        lInfNormData = normalizer.transform(df, {normalizer.p: float(x)})


# In[ ]:





# In[ ]:




