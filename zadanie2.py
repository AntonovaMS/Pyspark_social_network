import sys
import datetime
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField,IntegerType, StructType,StringType,LongType,DoubleType,FloatType,TimestampType
from pyspark import SparkContext
from pyspark.sql.functions import col,lit
from pyspark.sql.functions import rank
from pyspark.sql.functions import from_utc_timestamp
from pyspark.sql.functions import concat,col
from pyspark.sql.functions import *

def main():
    date = sys.argv[1]
    depth = sys.argv[2]
    base_input_path = sys.argv[3] #/user/master/data/geo/events
    geo_table = sys.argv[4] #/user/antonovams/data/geo.csv
    base_output_path = sys.argv[6] #/user/antonovams/data/analytics

    conf = SparkConf().setAppName(f"VerifiedTagsCandidatesJob-{date}-d{depth}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    def input_paths(date, depth):
        dt = datetime.datetime.strptime(date, '%Y-%m-%d')
        return [f"{base_input_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]

    paths = input_paths(date, depth)
    dist = sql.read.parquet(*paths)
    dist = dist.select("event.message_from","event.datetime","lat","lon")\
            .filter(col("event_type")=="message")\
            .filter(col("event.datetime").isNotNull())

    #geo.csv to DF
    geo = sql.read.format("csv").option("header","true").option("delimiter", ";")\
            .load(f'{geo_table}')
    list = geo.rdd.map(lambda x : [x.id, x.city, x.lat, x.lng, x.timezone]).collect()
    for i in list:
        i[2] = float(i[2].replace(",","."))
        i[3] = float(i[3].replace(",","."))

    geo_df=sql.createDataFrame(list, ['id','city','lat_geo','lon_geo','timezone'])

    mergedf= dist.alias("dist").crossJoin(geo_df.alias("g"))

    #таблица расчета расстояния от точки до точки
    table=( mergedf.select("dist.message_from","dist.datetime","g.city","g.lat_geo","dist.lat","g.lon_geo","dist.lon","g.timezone")\
    .withColumn("d",
                
            (2*6371*(F.asin(F.sqrt((F.pow( (F.cos((F.radians(col('lat'))-F.radians(col('lat_geo')))/F.lit(2))),F.lit(2)))
                +(F.cos(F.radians(col('lat_geo')))*F.cos(F.radians(col('lat')))*(F.pow( (F.cos((F.radians(col('lon'))-F.radians(col('lon_geo')))/F.lit(2))),F.lit(2)))))))) ))
    
    table = table.select(col("dist.message_from").alias("user_id"),"dist.datetime","g.city","d","g.timezone") 

    windowSpec3  = Window.partitionBy("user_id","datetime").orderBy(col("d").asc()) #все сообщения с городами
    tab = table.withColumn("row",row_number().over(windowSpec3)).filter(col("row") == 1).select("user_id","datetime","city","timezone")



    #город, из которого было отправлено последнее сообщение
    windowSpec1  = Window.partitionBy("user_id").orderBy(col("datetime").desc())
    tab_act_city = tab.withColumn("row",row_number().over(windowSpec1)).filter(col("row") == 1)
    tab_act_city = tab_act_city.select("user_id",col("city").alias("act_city"))  
    
        
    #home_city 
    tab_date = tab.select("user_id","city","datetime").withColumn("date",to_date("datetime"))#перевожу дtйттайм в дату
    last_mes_day = Window.partitionBy("user_id","city","date").orderBy(col("datetime").desc())#последнее сообщение за дату 
    tab_date = tab_date.withColumn("rn", row_number().over(last_mes_day))\
                    .withColumn("dt", F.year("date")*10000 + F.month("date")*100+F.dayofmonth("date"))\
                    .withColumn("grp",col("dt")-col("rn"))
    
    home_city = tab_date.select("user_id","city","grp").groupby("user_id","city","grp").count().filter(col("count")==28)
    w = Window.partitionBy("user_id","city")
    home_city = home_city.withColumn('maxgrp', F.max('grp').over(w)).filter(F.col('grp') == F.col('maxgrp')).drop('maxgrp')
    home_city = home_city.select("user_id",F.col("city").alias("home_city"))


    #travel_array 
    tab_date = tab_date.select("user_id","city","grp").groupby("user_id","city","grp").count()
    travel_array = tab_date.groupby("user_id").agg(F.collect_list("city").alias("travel_array"))

    #travel count
    travel_count = tab_date.groupBy("user_id").agg(F.count("city").alias("travel_count"))

    #local_time   
    win = Window.partitionBy("user_id").orderBy(col("datetime").desc())
    local_time = tab.select("user_id", "datetime","timezone").withColumn("row",row_number().over(win)).filter(col("row") == 1)\
                .withColumn("local_time",F.from_utc_timestamp(F.col("datetime"),F.col('timezone')))
    local_time = local_time.select("user_id","local_time")


    mt = tab_act_city.alias("tac").join(home_city.alias("hc"), tab_act_city.user_id == home_city.user_id,"left")
    mt = mt.select("tac.user_id","tac.act_city","hc.home_city")

    mt2 = mt.join(travel_array.alias("ta"),mt.user_id == travel_array.user_id ,"left")
    mt2 = mt2.select("tac.user_id","tac.act_city","hc.home_city","ta.travel_array")

    mt3 = mt2.join(travel_count.alias("tc"), mt2.user_id == travel_count.user_id ,"left")
    mt3 = mt3.select("tac.user_id","tac.act_city","hc.home_city","ta.travel_array","tc.travel_count")

    mt4 = mt3.join(local_time.alias("lt"),  mt3.user_id == local_time.user_id ,"left" )
    mt4 = mt4.select("tac.user_id","tac.act_city","hc.home_city","ta.travel_array","tc.travel_count","lt.local_time")
    mt4.write.parquet(f"{base_output_path}/date={date}")

if __name__ == "__main__":
        main()






