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



root = 'hdfs://wolf.iems.private/user/ttandon'
path = '{root}/HW4/venmo/venmoSample.csv.gz'.format(root=root)

print("ok1__________________________________________________________________________________")
sc = SparkContext()

#initialize sqlcontext
sqlcontext = SQLContext(sc)
venmo = sqlcontext.read.csv(path, header = True)
venmo.registerTempTable('venmo')



#Q.2.1 _____________________________________________________



# Define the function you want to return
def extract(s):
    all_matches = re.findall(r'[\U00010000-\U0010ffff]', s)
    return all_matches
# Create the UDF, note that you need to declare the return schema matching the returned type
extract_udf = udf(extract, ArrayType(StringType()))
df2 = venmo.where(col('description').isNotNull())
df2 = df2.withColumn('emoji', extract_udf('description'))
mvv_list = df2.select('emoji')
emoji_df = df2.select(explode(col('emoji'))\
	.alias('emo'))\
	.groupBy('emo')\
	.count().alias('count')\
	.orderBy('count', ascending = False)\
	.limit(10)\
	.toPandas()
emoji_df.to_csv("exercise2_1.csv", index = False)


#Q 2.2 _____________________________________________________________________

df2 = df2.withColumn( 'Date', to_timestamp(df2.datetime, 'yyyy-MM-dd HH:mm:ss')).drop("datetime")
df2 = df2.withColumn( "Date_week", date_format((df2.Date),'EEEE' ))
df3 = df2.withColumn("emoji_list", explode("emoji"))
df4 = df3.groupby("Date_week", "emoji_list").count().orderBy("Date_week", "count" , ascending=False).toPandas()
df4 = df4.groupby("Date_week").head(5)
df5 = df4[0:34]
df5.to_csv("exercise2_2.csv", index = False)

#Q 2.3 ______________________________________________

import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
df2 = df2.withColumn("len_desc",F.length("description") )
df2 = df2.withColumn("no_emoji", size("emoji"))

df2 = df2.withColumn("description", lower(col("description")))
df2 = df2.withColumn("food_flag", df2.description.like("food").cast('integer'))
df2 = df2.withColumn("uber_flag", df2.description.like("uber").cast('integer'))
df2 = df2.withColumn("fantasy_flag", df2.description.like("fantasy").cast('integer'))
df2 = df2.withColumn("gas_flag", df2.description.like("gas").cast('integer'))
df2 = df2.withColumn("rent_flag", df2.description.like("rent").cast('integer'))
df2 = df2.withColumn("per_emoji", ( size("emoji"))/ F.length("description")) 



vecAssembler = VectorAssembler(inputCols=["len_desc", "no_emoji", "food_flag", "uber_flag", "fantasy_flag", "gas_flag", "rent_flag", "per_emoji"], outputCol="features")
new_df = vecAssembler.transform(df2)


kmeans = KMeans(k=4, seed=1)  # 4 clusters here
model = kmeans.fit(new_df.select('features'))
transformed = model.transform(new_df)

#no of people in cluster
cluster_size = transformed.select("prediction").groupby("prediction").count().toPandas()
cluster_size.to_csv( "Cluster_split.csv", index = False )

centers = model.clusterCenters()

df = pd.DataFrame(index=["len_desc", "no_emoji", "food_flag", "uber_flag", "fantasy_flag", "gas_flag", "rent_flag", "per_emoji"], columns=['clust1','clust2','clust3','clust4', 'clust5'])
df['clust1'] = centers[0]
df['clust2'] = centers[1]
df['clust3'] = centers[2]
df['clust4'] = centers[3]
df['clust5'] = centers[3]

plt.bar(df.index, df['clust1'])
plt.title('means for cluster 1')
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig('exercise2_3_Cluster1.png')
plt.close()

plt.bar(df.index, df['clust2'])
plt.title('means for cluster 2')
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig('exercise2_3_Cluster2.png')
plt.close()

plt.bar(df.index, df['clust3'])
plt.title('means for cluster 3')
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig('exercise2_3_Cluster3.png')
plt.close()

plt.bar(df.index, df['clust4'])
plt.title('means for cluster 4')
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig('exercise2_3_Cluster4.png')
plt.close()

