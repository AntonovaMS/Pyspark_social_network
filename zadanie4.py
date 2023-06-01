import sys
import datetime
import pyspark.sql.functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
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
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import greatest,col
from pyspark.sql.functions import least,col

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


    #ПОДПИСАННЫ НА ОДНУ ГРУППУ
    dist1 = dist.select("event.user","event.subscription_channel","event_type").filter(col("event_type") == "subscription")
    merge1 = dist1.alias("t1").crossJoin(dist1.alias("t2"))
    merge1 = merge1.select("t1.user","t1.subscription_channel","t2.user","t2.subscription_channel")\
                .filter(col("t1.subscription_channel")==col("t2.subscription_channel")) 
    merge1 = merge1.withColumn('user_1', col("t1.user").cast("int")).withColumn('user_2', col("t2.user").cast("int"))
    merge1 = merge1.withColumn('user_left', F.greatest(col('user_1'), col('user_2')))\
                .withColumn('user_right', F.least(col('user_1'), col('user_2')))
    same_subscrip = merge1.select("user_left", "user_right").groupby("user_left", "user_right").count().drop("count")



    #уникальные пары переписывавшихся пользователей
    dist2 = dist.select("event.message_from","event.message_to").filter(col("event_type") == "message")


    dist2 = dist2.withColumn("user_1",col("num.message_to").cast("int")).withColumn("user_2",col("num.message_from").cast("int"))
    dist2 = dist2.withColumn('user_left', F.greatest(col("user_1"),col("user_2")))\
                .withColumn('user_right', F.least(col("user_1"),col("user_2")))
    no_message = dist2.select("user_left", "user_right").groupby("user_left", "user_right").count().drop("count")


    #РАССТОЯНИЕ МЕЖДУ НИМИ МЕНЕЕ 1 КМ

    dist3 = dist.select("event.user","event.message_from","event.datetime","lat","lon")\
            .filter(col("lat").isNotNull())\
            .withColumn("user_id", concat(coalesce("user", lit('')), coalesce("message_from", lit(''))))\
            .filter(col("user_id")!="").drop("user").drop("message_from") #все ивенты с широтой и долготой
    mergedf= dist3.alias("t1").crossJoin(dist3.alias("t2"))

    table=( mergedf.select("t1.user_id","t1.datetime","t1.lat","t1.lon","t2.user_id","t2.datetime","t2.lat","t2.lon")\
    .withColumn("d",
                
            (2*6371*(F.asin(F.sqrt((F.pow( (F.cos((F.radians(col('t2.lat'))-F.radians(col('t1.lat')))/F.lit(2))),F.lit(2)))
                +(F.cos(F.radians(col('t1.lat')))*F.cos(F.radians(col('t2.lat')))*(F.pow( (F.cos((F.radians(col('t2.lon'))-F.radians(col('t1.lon')))/F.lit(2))),F.lit(2)))))))) ))\
            .filter(col("d")<1) #фильтруем меньше 1 км 
    # айди с широтой и долготой
    table = table.select("t1.user_id","t1.datetime","t1.lat","t1.lon","t2.user_id")
    merge_tab = table.withColumn("user_1",col("t1.user_id").cast("int")).withColumn("user_2",col("t2.user_id").cast("int"))
    merge_tab = merge_tab.withColumn("user_left"F.greatest(col("user_2"),col("user_1")))\
                .withColumn('user_right', F.least(col("user_2"),col("user_1")))
    one_km = merge_tab.select("user_left", "user_right").groupby("user_left", "user_right").count().drop("count")


    #соединяем условия
    all_conditions = same_subscrip.alias("ss").join(no_message.alias("nos"), (col("ss.user_left") == col("nos.user_left")) & (col("ss.user_right") == col("nos.user_right")), "leftanti")
    all_conditions = all_conditions.select("ss.user_left","ss.user_right")
    all_conditions2 = all_conditions.alias("a").join(one_km.alias("o"), (col("a.user_left") == col("o.user_left")) & (col("a.user_right") == col("o.user_right")), "inner")
    all_conditions2 = all_conditions2.select("a.user_left","a.user_right")


    #zone_id город одного из юзеров
    table2 = table.select("t1.user_id","t1.datetime","t1.lat","t1.lon")
    city = all_conditions2.alias("ac2").join(table2.alias("t"), (col("user_left") == col("t.user_id")) | (col("user_right") == col("t.user_id")), "left")
    city = city.select("user_left","user_right","datetime" ,"lat","lon")


    #geo.csv to DF
    geo = sql.read.format("csv").option("header","true").option("delimiter", ";")\
                .load(f'{geo_table}')
    list = geo.rdd.map(lambda x : [x.id, x.city, x.lat, x.lng, x.timezone]).collect()
    for i in list:
        i[2] = float(i[2].replace(",","."))
        i[3] = float(i[3].replace(",","."))
    geo_df=sql.createDataFrame(list, ['id','city','lat_geo','lon_geo','timezone'])


    mergedf = city.alias("city").crossJoin(geo_df.alias("g"))

    #таблица расчета расстояния от локации юзера до центра города
    table=( mergedf.select("city.user_left","city.user_right","city.datetime" ,"city.lat","city.lon","g.city","g.timezone",'g.lat_geo','g.lon_geo')\
        .withColumn("d",
                    
                (2*6371*(F.asin(F.sqrt((F.pow( (F.cos((F.radians(col('lat'))-F.radians(col('lat_geo')))/F.lit(2))),F.lit(2)))
                    +(F.cos(F.radians(col('lat_geo')))*F.cos(F.radians(col('lat')))*(F.pow( (F.cos((F.radians(col('lon'))-F.radians(col('lon_geo')))/F.lit(2))),F.lit(2)))))))) ))

    windowSpec3  = Window.partitionBy("city.user_left","city.user_right").orderBy(col("d").asc())
    table = table.withColumn("row",row_number().over(windowSpec3)).filter(col("row") == 1)\
                .select("city.user_left","city.user_right","city.datetime" ,"g.city","g.timezone")
    table = table.select("city.user_left","city.user_right",F.from_utc_timestamp(F.col("city.datetime"),F.col('g.timezone')).alias("local_time"),col("g.city").alias("zone_id"))\
                .withColumn("processed_dttm", F.current_date())

    table.write.parquet(f"{base_output_path}/date={date}")

if __name__ == "__main__":
        main()
