import pyspark
from pyspark.sql.session import SparkSession
from datetime import timedelta, datetime
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import json
import boto3
import time
from pyspark.sql.types import (
    ArrayType,
    StructField,
    StructType,
    StringType,
    IntegerType,
    LongType,
)


spark = SparkSession.builder.appName(
    "customer affninity score calculation"
).getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


redshift_client = boto3.client("redshift-data", region_name="ap-southeast-2")


# read parquet files from s3 into dataframe

s3_product_file_path = "s3://ac-shopping-redshift-export/product/*.parquet"
product_df = spark.read.parquet(s3_product_file_path)
product_df = product_df.filter(product_df["brand_name"] != "")
product_df.createOrReplaceTempView("product_table")

s3_order_file_path = "s3://ac-shopping-redshift-export/order/*/*/*/*.parquet"
order_df = spark.read.parquet(s3_order_file_path)
order_df.createOrReplaceTempView("order_table")

# use boto3 client to execute statement
redshift_client = boto3.client("redshift-data", region_name="ap-southeast-2")

# parameters of execute_statement
query_response = redshift_client.execute_statement(
    ClusterIdentifier="ac-shopping-datawarehouse",
    Database="data_lake",
    SecretArn="arn:aws:secretsmanager:ap-southeast-2:721495903582:secret:redshift_ac_master-S6IgWp",
    Sql="""select recency_lower_bound,coalesce(recency_upper_bound,'2999-01-01'::timestamp) recency_upper_bound,
    order_count_lower_bound,coalesce(order_count_upper_bound,9999999999999) order_count_upper_bound,score from referrence.customer_affinity_score_mapping""",
)

# when passing query, we need to check status first, decribe statement to get status
query_status = redshift_client.describe_statement(Id=query_response["Id"])

# check whether query status become finished, otherwise wait till status change
while query_status["Status"] not in ["FINISHED", "ABORTED", "FAILED"]:
    time.sleep(5)
    query_status = redshift_client.describe_statement(Id=query_response["Id"])

# fetch statement result
mapping_data = redshift_client.get_statement_result(Id=query_response["Id"])

# print(mapping_data["Records"])
# return list of dictionares: name & datatype

mapping_data_schema = StructType(
    [
        StructField("recency", StringType(), False),
        StructField("order_count", StringType(), False),
        StructField("score", LongType(), True),
    ]
)

# Create data frame

# 1.'for' loop to create list
# column_list = []
# for column in mapping_data["ColumnMetadata"]:
#    print(column["name"])
#    column_list.append(column["name"])

# 2.'for' loop to create list

column_list = [c["name"] for c in mapping_data["ColumnMetadata"]]

##print(column_list)

# change valus in every item in rows to be one list
# five values in every row to be a tuple

rows = []
for record in mapping_data["Records"]:

    tmp = [list(r.values())[0] for r in record]

    rows.append(tmp)


# print(rows)

## convert using spark create data frame
affinity_score_mapping_df = spark.createDataFrame(rows).toDF(*column_list)
# affinity_score_mapping_df.printSchema()

# pandas is single thread, all of the data saved in one node


affinity_score_mapping_df.createOrReplaceTempView("affinity_score_mapping")

# calculate affinity score

customer_affinity_score_df = spark.sql(
    """with x as ( 
             select    o.customer_id,
                       o.order_id,
                       o.product_id,
                       order_datetime,
                       'brand' as affinity_type,
                       p.brand_name as affinity
                from order_table o
                join product_table p on o.product_id = p.product_id
                
            union all

         select    o.customer_id,
                   o.order_id,
                   o.product_id,
                   order_datetime,
                   'category_level_1' as affinity_type,
                   p.category_level_1 as affinity
                from order_table o
                join product_table p on o.product_id = p.product_id
                
           union all

         select    o.customer_id,
                   o.order_id,
                   o.product_id,
                   order_datetime,
                   'category_level_2' as affinity_type,
                   p.category_level_2 as affinity
                from order_table o
                join product_table p on o.product_id = p.product_id
               
         ),
        x2 as (
            select customer_id,
                   affinity_type,
                   affinity,
                   m.recency_lower_bound,
                   m.recency_upper_bound,
                   count(distinct order_id) as order_count
            from x join affinity_score_mapping m on x.order_datetime between recency_lower_bound and recency_upper_bound
            group by customer_id, affinity_type, affinity, m.recency_lower_bound, m.recency_upper_bound
        )


        select
            customer_id,
            affinity_type,
            affinity,
            sum(score) total,
            rank() over (partition by customer_id,affinity_type order by sum(score) desc) as ranking
        from x2
        join affinity_score_mapping m on
        x2.order_count >= m.order_count_lower_bound and x2.order_count<m.order_count_upper_bound
        and x2.recency_lower_bound = m.recency_lower_bound and x2.recency_upper_bound = m.recency_upper_bound
        group by customer_id, affinity_type, affinity;"""
)

# customer_affinity_score_df.show(100)
customer_affinity_score_df.write.parquet(
    "s3://ac-shopping-affinity-model/ac_shopping_customer_affinity_score/",
    mode="overwrite",
)

