#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from utils import *
from config import *

spark = SparkSession \
    .builder \
    .appName("BANK TRANSACTIONS BRONZE LAYER") \
    .getOrCreate()

print("Storage Location from Config File: ", storageLocation)


### RECREATE DATABASE AND TRX TABLE
spark.sql("DROP DATABASE IF EXISTS SPARK_CATALOG.HOL_DB_{} CASCADE".format(username))
spark.sql("CREATE DATABASE IF NOT EXISTS SPARK_CATALOG.HOL_DB_{}".format(username))


#---------------------------------------------------
#               CREATE PII TABLE
#---------------------------------------------------

### PII DIMENSION TABLE
piiDf = spark.read.options(header='True', delimiter=',').csv("{0}/pii/{1}/pii".format(storageLocation, username))

### CAST LAT LON AS FLOAT
piiDf = piiDf.withColumn("address_latitude",  piiDf["address_latitude"].cast('float'))
piiDf = piiDf.withColumn("address_longitude",  piiDf["address_longitude"].cast('float'))

### STORE CUSTOMER DATA AS TABLE
piiDf.writeTo("spark_catalog.HOL_DB_{0}.CUST_TABLE_{0}".format(username)).using("iceberg").createOrReplace()


#---------------------------------------------------
#               CREATE REFINED CUSTOMER TABLE
#---------------------------------------------------

spark.sql("DROP TABLE IF EXISTS SPARK_CATALOG.HOL_DB_{0}.CUST_TABLE_REFINED_{0}".format(username))

spark.sql("""CREATE TABLE SPARK_CATALOG.HOL_DB_{0}.CUST_TABLE_REFINED_{0}
                USING iceberg
                AS SELECT NAME, EMAIL, BANK_COUNTRY, ACCOUNT_NO, CREDIT_CARD_NUMBER, ADDRESS_LATITUDE, ADDRESS_LONGITUDE
                FROM SPARK_CATALOG.HOL_DB_{0}.CUST_TABLE_{0}""".format(username))


#---------------------------------------------------
#               SCHEMA EVOLUTION
#---------------------------------------------------

# UPDATE TYPES: Updating Latitude and Longitude FROM FLOAT TO DOUBLE
spark.sql("""ALTER TABLE SPARK_CATALOG.HOL_DB_{0}.CUST_TABLE_REFINED_{0}
                ALTER COLUMN ADDRESS_LATITUDE TYPE double""".format(username))

spark.sql("""ALTER TABLE SPARK_CATALOG.HOL_DB_{0}.CUST_TABLE_REFINED_{0}
                ALTER COLUMN ADDRESS_LONGITUDE TYPE double""".format(username))


#---------------------------------------------------
#               VALIDATE TABLE
#---------------------------------------------------

spark.sql("""SELECT * FROM SPARK_CATALOG.HOL_DB_{0}.CUST_TABLE_REFINED_{0}""".format(username)).show()


#---------------------------------------------------
#               CREATE TRANSACTIONS TABLE
#---------------------------------------------------

### LOAD HISTORICAL TRANSACTIONS FILE FROM CLOUD STORAGE
transactionsDf = spark.read.json("{0}/transactions/{1}/rawtransactions".format(storageLocation, username))
transactionsDf.printSchema()

### RUN PYTHON FUNCTION TO FLATTEN NESTED STRUCTS AND VALIDATE NEW SCHEMA
transactionsDf = transactionsDf.select(flatten_struct(transactionsDf.schema))
transactionsDf.printSchema()

### RENAME MULTIPLE COLUMNS
cols = [col for col in transactionsDf.columns if col.startswith("transaction")]
new_cols = [col.split(".")[1] for col in cols]
transactionsDf = renameMultipleColumns(transactionsDf, cols, new_cols)

### CAST TYPES
cols = ["transaction_amount", "latitude", "longitude"]
transactionsDf = castMultipleColumns(transactionsDf, cols)
transactionsDf = transactionsDf.withColumn("event_ts", transactionsDf["event_ts"].cast("timestamp"))

### SAVE TRANSACTIONS AS TABLE
transactionsDf.writeTo("SPARK_CATALOG.HOL_DB_{0}.HIST_TRX_{0}".format(username))\
                .using("iceberg")\
                .tableProperty("write.format.default", "parquet")\
                .createOrReplace()

print("COUNT OF TRANSACTIONS TABLE")
spark.sql("SELECT COUNT(*) FROM SPARK_CATALOG.HOL_DB_{0}.HIST_TRX_{0};".format(username)).show()


#---------------------------------------------------
#               CREATE TRANSACTIONS BRANCH
#---------------------------------------------------

### TRANSACTIONS FACT TABLE
trxBatchDf = spark.read.json("{0}/transactions/{1}/trx_batch_2".format(storageLocation, username))

### TRX DF SCHEMA BEFORE CASTING
trxBatchDf.printSchema()

### CAST TYPES
cols = ["transaction_amount", "latitude", "longitude"]
trxBatchDf = castMultipleColumns(trxBatchDf, cols)
trxBatchDf = trxBatchDf.withColumn("event_ts", trxBatchDf["event_ts"].cast("timestamp"))

# CREATE TABLE BRANC
spark.sql("ALTER TABLE SPARK_CATALOG.HOL_DB_{0}.HIST_TRX_{0} CREATE BRANCH ingestion_branch".format(username))

# WRITE DATA OPERATION ON TABLE BRANCH
trxBatchDf.write.format("iceberg").option("branch", "ingestion_branch").mode("append").save("SPARK_CATALOG.HOL_DB_{0}.HIST_TRX_{0}".format(username))
