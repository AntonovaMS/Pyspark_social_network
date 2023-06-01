
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

    paths = input_paths(date,  depth)
    dist = sql.read.parquet(*paths)
    dist = dist.select("event.message_from","event.datetime","lat","lon","event_type").filter(col("event.datetime").isNotNull())

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
    table=( mergedf.select("dist.message_from","dist.datetime","g.city","g.lat_geo","dist.lat","g.lon_geo","dist.lon","dist.event_type")\
    .withColumn("d",
                
            (2*6371*(F.asin(F.sqrt((F.pow( (F.cos((F.radians(col('lat'))-F.radians(col('lat_geo')))/F.lit(2))),F.lit(2)))
                +(F.cos(F.radians(col('lat_geo')))*F.cos(F.radians(col('lat')))*(F.pow( (F.cos((F.radians(col('lon'))-F.radians(col('lon_geo')))/F.lit(2))),F.lit(2)))))))) ))



    #MESSAGES WEEK AND MONTH
    tab_messages = table.filter(col("event_type")=="message")
    windowSpec3  = Window.partitionBy("datetime").orderBy(col("d").asc()) #все сообщения с городами
    tab_messages = tab_messages.withColumn("row",row_number().over(windowSpec3)).filter(col("row") == 1).select("city","datetime","event_type")

    message = tab_messages.select((date_trunc("month", "datetime").alias("month")),(date_trunc("week", "datetime").alias("week")),"city","event_type")
    messages_week = message.groupby("month","week","city").count().alias("week_message")
    messages_month = message.groupby("month","city").count().alias("month_message")

    all_messages = messages_month.alias("mm").join(messages_week.alias("mw"),( messages_week.month ==  messages_month.month) & (messages_week.city ==  messages_month.city),"left")
    all_messages = all_messages.select("mm.month","mw.week","mm.city",col("mm.count").alias("month_message"),col("mw.count").alias("week_message"))
        


    #REACTION WEEK AND MONTH
    tab_reaction = table.filter(col("event_type")=="reaction")
    windowSpec3  = Window.partitionBy("datetime").orderBy(col("d").asc()) 
    tab_reaction = tab_reaction.withColumn("row",row_number().over(windowSpec3)).filter(col("row") == 1).select("city","datetime","event_type")

    reaction = tab_reaction.select((date_trunc("month", "datetime").alias("month")),(date_trunc("week", "datetime").alias("week")),"city","event_type")
    reaction_week = reaction.groupby("month","week","city").count().alias("week_reaction")
    reaction_month = reaction.groupby("month","city").count().alias("month_reaction")

    all_reaction = reaction_month.alias("rm").join(reaction_week.alias("rw"),( reaction_week.month ==  reaction_month.month) & (reaction_week.city ==  reaction_month.city),"left")
    all_reaction = all_reaction.select("rm.month","rw.week","rm.city",col("rm.count").alias("month_reaction"),col("rw.count").alias("week_reaction"))


    #subscription WEEK AND MONTH
    tab_subscription = table.filter(col("event_type")=="subscription")
    windowSpec3  = Window.partitionBy("datetime").orderBy(col("d").asc()) 
    tab_subscription = tab_subscription.withColumn("row",row_number().over(windowSpec3)).filter(col("row") == 1).select("city","datetime","event_type")

    subscription = tab_subscription.select((date_trunc("month", "datetime").alias("month")),(date_trunc("week", "datetime").alias("week")),"city","event_type")
    subscription_week = subscription.groupby("month","week","city").count().alias("week_subscription")
    subscription_month = subscription.groupby("month","city").count().alias("month_subscription")

    all_subscription = subscription_month.alias("sm").join(subscription_week.alias("sw"),( subscription_week.month ==  subscription_month.month) & (subscription_week.city ==  subscription_month.city),"left")
    all_subscription = all_subscription.select("sm.month","sw.week","sm.city",col("sm.count").alias("month_subscription"),col("sw.count").alias("week_subscription"))


    #РЕГИСТРАЦИИ
    tab_messages = table.filter(col("event_type")=="message")
    windowSpec3  = Window.partitionBy("datetime").orderBy(col("d").asc()) 
    tab_messages = tab_messages.withColumn("row",row_number().over(windowSpec3)).filter(col("row") == 1).select("message_from","city","datetime","event_type")
    message = tab_messages.select((date_trunc("month", "datetime").alias("month")),(date_trunc("week", "datetime").alias("week")),"message_from","city","event_type")
    week_user = message.groupby("month","week","city").agg(countDistinct('message_from').alias("week_user"))
    month_user = message.groupby("month","city").agg(countDistinct('message_from').alias("month_user"))


    all_user = month_user.alias("mu").join(week_user.alias("wu"),(week_user.month ==  month_user.month) & (week_user.city ==  month_user.city),"left")
    all_user = all_user.select("mu.month","wu.week","mu.city",col("mu.month_user"),col("wu.week_user"))

    #ДЖОИНИМ ТАБЛИЦЫ
    merged_table = all_messages.alias("am").join(all_reaction.alias("ar"), (all_messages.month == all_reaction.month)&(all_messages.week == all_reaction.week)&(all_messages.city == all_reaction.city),"left")
    merged_table1 = merged_table.select("am.month","am.week","am.city","am.month_message","am.week_message","ar.month_reaction","ar.week_reaction")

    mt2 = merged_table1.alias("mt1").join(all_subscription.alias("as"),(merged_table1.month == all_subscription.month)&(merged_table1.week == all_subscription.week)&(merged_table1.city == all_subscription.city),"left")
    mt2 = mt2.select("mt1.month","mt1.week","mt1.city","mt1.month_message","mt1.week_message","mt1.month_reaction","mt1.week_reaction","as.month_subscription","as.week_subscription")

    mt3 = mt2.alias("mt2").join(all_user.alias("au"),(mt2.month == all_user.month)&(mt2.week == all_user.week)&(mt2.city == all_user.city),"left")
    mt3 = mt3.select("mt2.month","mt2.week","mt2.city","mt2.month_message","mt2.week_message","mt2.month_reaction","mt2.week_reaction","mt2.month_subscription","mt2.week_subscription","au.month_user","au.week_user")
    
    mt3.write.parquet(f"{base_output_path}/date={date}")

if __name__ == "__main__":
        main()