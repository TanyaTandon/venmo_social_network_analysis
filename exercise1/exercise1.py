from pyspark.sql.types import *
from pyspark.sql.functions import *
import re
import pandas as pd
import numpy as np
from pyspark.sql import SQLContext
from pyspark import SparkContext
import matplotlib.pyplot as plt
import pyspark.sql.functions as f
from itertools import chain
from pyspark.sql.types import DateType




root = 'hdfs://wolf.iems.private/user/ttandon'
path = '{root}/HW4/venmo/venmoSample.csv.gz'.format(root=root)

print("ok1__________________________________________________________________________________")
sc = SparkContext()

#initialize sqlcontext
sqlcontext = SQLContext(sc)
venmo = sqlcontext.read.csv(path, header = True)
venmo.registerTempTable('venmo')
print("ok2__________________________________________________________________________________")
v1 = venmo.select("user1", "user2").drop_duplicates()
print("ok3__________________________________________________________________________________")
# Q1.2 outdegree distribution __________________________________________________________________________________________________
v2 = v1.groupby("user1").count()
v2 = v2.selectExpr("user1 as user1", "count as degree")
v3 = v2.toPandas()
plt.hist( v3["degree"])
plt.savefig("hist_Outdegree.png")

# Q1.2 indegree distribution __________________________________________________________________________________________________

v11 = v1.groupby("user2").count()
v11 = v11.selectExpr("user2 as user2", "count as degree")
v12 = v11.toPandas()
plt.hist(v12["degree"])
plt.savefig("hist_InDegree.png")

# Q1.1 Degree distribution__________________________________________________________________________________________________
v1 = venmo.select("user1", "user2").drop_duplicates()
v5 = venmo.select("user2", "user1").drop_duplicates()
v6 = v1.union(v5).drop_duplicates()
v7 = v6.groupby("user1").count()
v8 = v7.selectExpr("user1 as user1", "count as degree")
v10 = v8.toPandas()
plt.hist(v10["degree"])
plt.savefig("hist_Degree.png")

# Q1.3 The percentage of reciprocal transactions in the network __________________________________________________________________________________________________
v14 = v1.union(v5)
v15 = v14.groupby("user1", "user2").count()
v16 = v15.filter(f.col("count") == 2)
per = (v16.count() / v15.count()) * 100
print(per)
print( "Result--------------------------------------------------------------------------------------------------")
#np.savetxt( "Percentage_of_reciprocal.txt", per, header = "Percentage of reciprocal transactions in the network") 

# Q1.3.2 The percentage of reciprocal transactions in the network per semester ________________________________________________________________________________________
venmo = venmo.withColumn( 'Date', to_timestamp(venmo.datetime, 'yyyy-MM-dd HH:mm:ss')).drop("datetime")
venmo = venmo.withColumn( "Date_month", month(venmo.Date))
venmo = venmo.withColumn( "Date_year", year(venmo.Date))
venmo = venmo.filter("Date_year is not NULL")

venmo = venmo.withColumn("Date_12",f.when(venmo.Date_month <= 6,1).otherwise(2))
venmo = venmo.filter("Date_12 is not NULL")
v1 = venmo.select("user1", "user2","Date_year", "Date_12" )
v1 = v1.select("user1", "user2","Date_12", "Date_year" ).drop_duplicates()



v2 = venmo.select("user2", "user1","Date_year", "Date_12" )
v2 = v2.select("user2", "user1","Date_12", "Date_year" ).drop_duplicates()

v14 = v1.union(v2)



v15 = v14.groupby("user1", "user2", "Date_12", "Date_year").count()
v16 = v15.filter(f.col("count") == 2)
v17 = v15.groupby( "Date_12", "Date_year").count()

v17 = v17.selectExpr("Date_12 as Date_12", "Date_year as Date_year", "count as count1").toPandas()
v18 = v16.groupby( "Date_12", "Date_year").count()
v18 = v18.selectExpr("Date_12 as Date_12", "Date_year as Date_year" ,"count as count2").toPandas()

v_merged = pd.merge(v17,v18, how='inner',left_on=["Date_12", "Date_year"],right_on=["Date_12", "Date_year"])

v_merged['percentage'] =( v_merged['count2']/v_merged['count1'] )* 100
v_merged["Semester:Year"] = v_merged["Date_12"].map(str) + " : " +  v_merged["Date_year"].map(str)
plt.bar(v_merged['Semester:Year'], v_merged['percentage'])
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig('exercise1_3.png')
