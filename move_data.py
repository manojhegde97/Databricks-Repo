from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from sys import platform as _platform
import time
from argparse import ArgumentParser
from datetime import datetime
from pyspark.sql.functions import udf, col, from_json, to_date, lit, desc, last
from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, explode
from pyfiles.utils import utils
from pyfiles.conf import config


def write_data(df, output_file):
    (df
     .write
     .option("header", True)
     .partitionBy("year", "month", "day")
     .mode("append")
     .parquet(output_file))
    return None


def read_from_postgres(spark, conf, args):
    postgres_user,postgres_key = None,None
    try:
        postgres_user, postgres_key = args.input_database_user,args.input_database_pass_key
        print(postgres_user, postgres_key)
    except:
        postgres_user, postgres_key = conf.POSTGRES_USER,conf.POSTGRES_SECRET
    if(postgres_user == None or postgres_key == None):
        postgres_user, postgres_key = conf.POSTGRES_USER, conf.POSTGRES_SECRET
    print(postgres_user,postgres_key)
    df = spark.read \
        .format("jdbc") \
        .option("url", args.input_database_url) \
        .option("dbtable", args.input_table_name) \
        .option("user", postgres_user) \
        .option("password", dbutils.secrets.get(scope=conf.SECRET_SCOPE, key=postgres_key)) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    df.show()
    return df


def read_from_adls(spark, conf, args):
    if (args.input_file_type == 'CSV'):
        return utils.read_data_from_csv(spark, args.input_source_path, dbutils)
    elif (args.input_file_type == 'PARQUET'):
        return utils.read_data_from_parquet(spark, args.input_source_path, dbutils)
    return None


def read_input(spark, conf, args):
    if args.input_source_type == 'POSTGRES':
        return read_from_postgres(spark, conf, args)
    elif (args.input_source_type == 'ADLS'):
        return read_from_adls(spark, conf, args)
    return None


def transform(df, mapping_config):
    mapping_config.show()
    renameDf = mapping_config.filter(col('CAST_TYPE') != 'list')
    explodeDf = mapping_config.filter(col('CAST_TYPE') == 'list')

    explode_list = explodeDf.collect()
    for row in explode_list:
        df = df.withColumn(row['TRANSFORMED_NAME'], explode(col(row['ORIGINAL_NAME'])))
    rename_list = renameDf.collect()
    mapping_transform = [col(row['ORIGINAL_NAME']).cast(row['CAST_TYPE']).alias(row['TRANSFORMED_NAME']) for row in
                         rename_list]
    return df.select(*mapping_transform)


def main(args):
    ## get config as per environment
    conf = utils.get_config_for_env(args.env, config)
    spark = utils.create_spark_session('avro_to_parquet')
    ## authenticate storage account
    utils.authenticate_storage_account(spark, conf,
                                       dbutils.secrets.get(scope=conf.SECRET_SCOPE, key=conf.DATALAKE_SECRET))
    ## reading input
    df = read_input(spark, conf, args)

    ##read mapping and selection config
    mapping_config = utils.read_data_from_csv(spark, args.mapping_path,dbutils)

    ##transform as per mapping config
    df = transform(df, mapping_config)

    df.show()
    print(df.schema)
    ## writing to output path
    (df
     .write
     .format("delta")
     .mode("overwrite")
     .save(args.output_path))


# entry point for PySpark ETL application
if __name__ == '__main__':
    parser = ArgumentParser(description='Move data')
    parser.add_argument(
        '--env', type=str, required=True,
        help='environment of job'
    )
    parser.add_argument(
        '--input_source_type', type=str, required=True,
        help='type of input source'
    )
    parser.add_argument(
        '--output_path', type=str, required=True, help='output data format'
    )
    parser.add_argument(
        '--mapping_path', type=str, required=True, help='column mapping path'
    )
    ## if input source type is data lake
    parser.add_argument(
        '--input_file_type', type=str, required=False,
        help='type of input source'
    )
    parser.add_argument(
        '--input_source_path', type=str, required=False,
        help='path of input source'
    )
    ## if input source is postgres database
    parser.add_argument(
        '--input_database_url', type=str, required=False,
        help='url of input source'
    )
    parser.add_argument(
        '--input_table_name', type=str, required=False,
        help='name of input table'
    )
    parser.add_argument(
        '--input_database_user', type=str, required=False,
        help='name of db user'
    )
    parser.add_argument(
        '--input_database_pass_key', type=str, required=False,
        help='database password key'
    )

    args = parser.parse_args()

main(args)