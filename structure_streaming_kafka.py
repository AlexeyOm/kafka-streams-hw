from pyspark.sql import SparkSession
from time import sleep
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import StructType, StringType, IntegerType

# явным образом задаем структуру json-контента
schema = StructType().add("id",IntegerType()).add("action", StringType())

users_schema = StructType().add("id",IntegerType()).add("user_name", StringType()).add("user_age", IntegerType())

spark = SparkSession.builder.appName("SparkStreamingKafka").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
# static dataset - эмуляция внешнего источника данных

input_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "netology") \
  .option("failOnDataLoss", False) \
  .load()



#покажем входящий контент
#input_stream.writeStream.format("console").outputMode("append").start().awaitTermination()
#input_stream = input_stream.writeStream.format("console").outputMode("append").start()







#разберем входящий контент из json
json_stream = input_stream.select(col("timestamp").cast("string"), from_json(col("value").cast("string"), schema).alias("parsed_value"))
#проверим что все ок
#json_stream.printSchema()
#json_stream.writeStream.format("console").outputMode("append").option("truncate", False).start().awaitTermination()

#выделем интересующие элементы
clean_data = json_stream.select(col("timestamp"), col("parsed_value.id").alias("id"), col("parsed_value.action").alias("action"))
#проверим
#clean_data.writeStream.format("console").outputMode("append").option("truncate", False).start().awaitTermination()

#добавим агрегат - отображать число уникальных айдюков
stat_stream = clean_data.groupBy("id").count()
#stat_stream.writeStream.format("console").outputMode("complete").option("truncate", False).start().awaitTermination()


#--далее усложним наш пример--

#добавим join с статическим dataset - создаем данные
users_data = [(1,"Jimmy",18),(2,"Hank",48),(3,"Johnny",9),(4,"Erle",40)]
users = spark.createDataFrame(data=users_data,schema=users_schema)
#users.repartition(1).write.csv("static/users","overwrite",header=True)

#делаем join
join_stream = stat_stream.join(users, stat_stream.id == users.id, "left_outer").select(users.user_name, users.user_age, col('count'))
#join_stream.writeStream.format("console").outputMode("complete").option("truncate", False).start().awaitTermination()




#убираем terminate
# join_stream.writeStream.\
#   format("console").\
#   outputMode("complete").\
#   option("truncate", False).\
#   option("checkpointLocation", "checkpoint/target").\
#   start()

#отображаем поток
temp1 = json_stream.select(col("timestamp"), col("parsed_value.id").alias("id"), col("parsed_value.action").alias("action"))
#temp1.coalesce(1).writeStream.format("json").option("path", "streaming/source").option("checkpointLocation", "checkpoint/source").outputMode("append").trigger(processingTime='10 seconds').start()

temp1.printSchema()

res = temp1.\
  join(users, temp1.id == users.id, "left_outer").\
  select(temp1.timestamp,temp1.id,users.user_name,users.user_age,temp1.action). \
  writeStream.format("console").option("checkpointLocation", "checkpoint/target").outputMode("append").start()
  # writeStream.format("json").option("path", "streaming/target").option("checkpointLocation", "checkpoint/target").outputMode("append").trigger(processingTime='60 seconds').start().awaitTermination(timeout=60)

#temp1.writeStream.format("json").option("path", "streaming").option("checkpointLocation", "checkpoint").outputMode("append").trigger(processingTime='10 seconds').start()

sleep(10)
res.stop()




