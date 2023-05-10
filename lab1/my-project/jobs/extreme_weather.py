"""
--conf spark.jars=/usr/lib/hudi/hudi-spark-bundle.jar --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
"""

try:
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    import sys, os, ast, uuid, boto3, time, re, json, hashlib
    from datetime import datetime, timezone
    from ast import literal_eval
    from dataclasses import dataclass
    from pyspark.sql.functions import lit, udf
    import math
    import threading
    from dateutil.parser import parse
    from dateutil.tz import tzutc
    from boto3.s3.transfer import TransferConfig

except Exception as e:
    print(e)

# Create a Spark session
spark = (
    SparkSession.builder.appName("SparkSQL")
    .enableHiveSupport()
    .getOrCreate()
)


class AWSS3(object):
    """Helper class to which add functionality on top of boto3 """

    def __init__(self, bucket):

        self.BucketName = bucket
        self.client = boto3.client("s3")

    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:
            response = self.client.put_object(
                Body=Response, Bucket=self.BucketName, Key=Key
            )
            return "ok"
        except Exception as e:
            raise Exception("Error : {} ".format(e))

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):

        """Gets the Bytes Data from AWS S3 """

        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()

        except Exception as e:
            print("Error :{}".format(e))
            return False

    def find_one_update(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.item_exists(Key=key)

        if flag:
            data = self.get_item(Key=key)
            return data

        else:
            self.put_files(Key=key, Response=data)
            return data

    def delete_object(self, Key):

        response = self.client.delete_object(Bucket=self.BucketName, Key=Key, )
        return response

    def get_folder_names(self, Prefix=""):
        """
        :param Prefix: Prefix string
        :return: List of folder names
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix, Delimiter='/')

            folder_names = []

            for page in pages:
                for prefix in page.get('CommonPrefixes', []):
                    folder_names.append(prefix['Prefix'].rstrip('/'))

            return folder_names
        except Exception as e:
            print("error", e)
            return []

    def get_ll_keys_with_meta_data_sorted(self, Prefix="", timestamp=None, sort_order='asc'):
        """
        :param Prefix: Prefix string
        :param timestamp: datetime object
        :param sort_order: 'asc' for ascending, 'desc' for descending
        :return: Sorted keys List with full S3 path
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)

            tmp = []

            for page in pages:
                for obj in page["Contents"]:
                    last_modified = obj["LastModified"].replace(tzinfo=timezone.utc)
                    if timestamp is None:
                        full_path = f's3://{self.BucketName}/{obj["Key"]}'
                        obj['full_path'] = full_path
                        tmp.append(obj)
                    else:
                        """filter out keys greater than datetime provided """
                        if last_modified > timestamp:
                            # Return full S3 path
                            full_path = f's3://{self.BucketName}/{obj["Key"]}'
                            obj['full_path'] = full_path
                            tmp.append(obj)
                        else:
                            pass
            # Sort the list based on LastModified value
            sorted_tmp = sorted(tmp, key=lambda x: x['LastModified'], reverse=(sort_order == 'desc'))
            return sorted_tmp
        except Exception as e:
            print("error", e)
            return []


class Checkpoints(AWSS3):
    def __init__(self, path):
        self.path = path
        self.bucket = self.path.split("/")[2]

        AWSS3.__init__(
            self, self.bucket
        )
        self.__data = {
            "process_time": datetime.now().__str__(),
            "last_processed_file_name": None,
            "last_processed_time_stamp_of_file": None,
            "last_processed_file_path": None,
            "last_processed_partition": None
        }
        self.prefix = self.path.split(f"{self.bucket}/")[1]

        self.filename = f"{hashlib.sha256(str(self.path).encode('utf-8')).hexdigest()}.json"
        self.meta_Data = []
        self.folders = None

    def __get_objects_each_folder(self, folder, timestamp=None):
        for item in self.get_ll_keys_with_meta_data_sorted(Prefix=folder, timestamp=timestamp):
            self.meta_Data.append(item)

    def read(self):
        if self.checkpoint_exists():
            read_check_points = self.read_check_point()
            print("read_check_points", json.dumps(read_check_points, indent=3))

            timestamp = parse(read_check_points.get("last_processed_time_stamp_of_file")).replace(tzinfo=timezone.utc)

            """Get the folder Names """
            self.folders = self.get_folder_names(Prefix=self.prefix)
            if self.folders != []:
                for folder in self.folders:
                    self.__get_objects_each_folder(folder=folder, timestamp=timestamp)
            else:
                self.__get_objects_each_folder(folder=self.prefix, timestamp=timestamp)

            return self.meta_Data

        else:
            self.folders = self.get_folder_names(Prefix=self.prefix)
            if self.folders != []:
                for folder in self.folders:
                    self.__get_objects_each_folder(folder=folder)
            else:
                self.__get_objects_each_folder(folder=self.prefix)
            return self.meta_Data

    def commit(self):
        if self.meta_Data != []:
            if self.folders != []:
                self.create_check_points(
                    last_processed_time_stamp_of_file=str(self.meta_Data[-1].get("LastModified")),
                    last_processed_file_name=self.meta_Data[-1].get("Key"),
                    last_processed_file_path=self.meta_Data[-1].get("full_path"),
                    last_processed_partition=self.folders[-1]
                )
            else:
                self.create_check_points(
                    last_processed_time_stamp_of_file=str(self.meta_Data[-1].get("LastModified")),
                    last_processed_file_name=self.meta_Data[-1].get("Key"),
                    last_processed_file_path=self.meta_Data[-1].get("full_path"),
                    last_processed_partition=None
                )

    def read_check_point(self):
        return json.loads(self.get_item(Key=self.filename).decode("utf-8"))

    def checkpoint_exists(self):
        return self.item_exists(Key=self.filename)

    def create_check_points(self, last_processed_time_stamp_of_file, last_processed_file_name,
                            last_processed_file_path, last_processed_partition):
        print(self.folders)
        self.__data['last_processed_time_stamp_of_file'] = last_processed_time_stamp_of_file
        self.__data['last_processed_file_name'] = last_processed_file_name
        self.__data['last_processed_partition'] = last_processed_partition

        self.put_files(
            Key=self.filename,
            Response=json.dumps(
                self.__data
            )
        )
        return True

    def delete_checkpoints(self):
        self.delete_object(Key=self.filename)


def upsert_hudi_table(glue_database, table_name,
                      record_id, precomb_key, partition_fields, table_type, spark_df,
                      enable_partition, enable_cleaner, enable_hive_sync, use_sql_transformer, sql_transformer_query,
                      target_path, index_type, method='upsert'):
    """
    Upserts a dataframe into a Hudi table.

    Args:
        glue_database (str): The name of the glue database.
        table_name (str): The name of the Hudi table.
        record_id (str): The name of the field in the dataframe that will be used as the record key.
        precomb_key (str): The name of the field in the dataframe that will be used for pre-combine.
        table_type (str): The Hudi table type (e.g., COPY_ON_WRITE, MERGE_ON_READ).
        spark_df (pyspark.sql.DataFrame): The dataframe to upsert.
        enable_partition (bool): Whether or not to enable partitioning.
        enable_cleaner (bool): Whether or not to enable data cleaning.
        enable_hive_sync (bool): Whether or not to enable syncing with Hive.
        use_sql_transformer (bool): Whether to use SQL to transform the dataframe before upserting.
        sql_transformer_query (str): The SQL query to use for data transformation.
        target_path (str): The path to the target Hudi table.
        method (str): The Hudi write method to use (default is 'upsert').
        index_type : BLOOM or GLOBAL_BLOOM
    Returns:
        None
    """
    # These are the basic settings for the Hoodie table
    hudi_final_settings = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": method,
        "hoodie.datasource.write.recordkey.field": record_id,
        "hoodie.datasource.write.precombine.field": precomb_key,
    }

    # These settings enable syncing with Hive
    hudi_hive_sync_settings = {
        "hoodie.parquet.compression.codec": "gzip",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": glue_database,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
    }

    # These settings enable automatic cleaning of old data
    hudi_cleaner_options = {
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async": "true",
        "hoodie.cleaner.policy": 'KEEP_LATEST_FILE_VERSIONS',
        "hoodie.cleaner.fileversions.retained": "3",
        "hoodie-conf hoodie.cleaner.parallelism": '200',
        'hoodie.cleaner.commits.retained': 5
    }

    # These settings enable partitioning of the data
    partition_settings = {
        "hoodie.datasource.write.partitionpath.field": partition_fields,
        "hoodie.datasource.hive_sync.partition_fields": partition_fields,
        "hoodie.datasource.write.hive_style_partitioning": "true",
    }

    # Define a dictionary with the index settings for Hudi
    hudi_index_settings = {
        "hoodie.index.type": index_type,  # Specify the index type for Hudi
    }

    # Add the Hudi index settings to the final settings dictionary
    for key, value in hudi_index_settings.items():
        hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    # If partitioning is enabled, add the partition settings to the final settings
    if enable_partition == "True" or enable_partition == "true" or enable_partition == True:
        for key, value in partition_settings.items(): hudi_final_settings[key] = value

    # If data cleaning is enabled, add the cleaner options to the final settings
    if enable_cleaner == "True" or enable_cleaner == "true" or enable_cleaner == True:
        for key, value in hudi_cleaner_options.items(): hudi_final_settings[key] = value

    # If Hive syncing is enabled, add the Hive sync settings to the final settings
    if enable_hive_sync == "True" or enable_hive_sync == "true" or enable_hive_sync == True:
        for key, value in hudi_hive_sync_settings.items(): hudi_final_settings[key] = value

    # If there is data to write, apply any SQL transformations and write to the target path
    if spark_df.count() > 0:
        if use_sql_transformer == "True" or use_sql_transformer == "true" or use_sql_transformer == True:
            spark_df.createOrReplaceTempView("temp")
            spark_df = spark.sql(sql_transformer_query)

        spark_df.write.format("hudi"). \
            options(**hudi_final_settings). \
            mode("append"). \
            save(target_path)


def run_job():
    path = "s3://jt-soumilshah-test/raw/"

    helper = Checkpoints(path=path)
    response = helper.read()

    if response != []:
        files_to_proces = [item.get("full_path", "") for item in response]
        print("files_to_proces", files_to_proces)

        spark_df = spark.read.json(files_to_proces)
        spark_df.printSchema()
        spark_df.show()
        print("********************")

        upsert_hudi_table(
            glue_database="hudidb",
            table_name="tbl_orders",
            record_id="orderid",
            precomb_key="orderid",
            partition_fields='default',
            table_type="COPY_ON_WRITE",
            method='upsert',
            index_type="BLOOM",
            enable_partition=False,
            enable_cleaner=False,
            enable_hive_sync=True,
            use_sql_transformer=False,
            sql_transformer_query="default",
            target_path="s3://jt-soumilshah-test/silver/",
            spark_df=spark_df,
        )
        print("****************Commit the pointer***********")
        helper.commit()


run_job()
