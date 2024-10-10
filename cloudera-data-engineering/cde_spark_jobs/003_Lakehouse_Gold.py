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
from config import storageLocation, username
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("BANK TRANSACTIONS GOLD LAYER") \
    .getOrCreate()

print("Storage Location from Config File: ", storageLocation)


#---------------------------------------------------
#               ICEBERG INCREMENTAL READ
#---------------------------------------------------

# ICEBERG TABLE HISTORY (SHOWS EACH SNAPSHOT AND TIMESTAMP)
spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.HIST_TRX_{0}.history".format(username)).show()

# ICEBERG TABLE SNAPSHOTS (USEFUL FOR INCREMENTAL QUERIES AND TIME TRAVEL)
spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.HIST_TRX_{0}.snapshots".format(username)).show()

# STORE FIRST AND LAST SNAPSHOT ID'S FROM SNAPSHOTS TABLE
snapshots_df = spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.HIST_TRX_{0}.snapshots;".format(username))

# SNAPSHOTS
snapshots_df.show()

last_snapshot = snapshots_df.select("snapshot_id").tail(1)[0][0]
#second_snapshot = snapshots_df.select("snapshot_id").collect()[1][0]
first_snapshot = snapshots_df.select("snapshot_id").head(1)[0][0]

incReadDf = spark.read\
    .format("iceberg")\
    .option("start-snapshot-id", first_snapshot)\
    .option("end-snapshot-id", last_snapshot)\
    .load("spark_catalog.HOL_DB_{0}.HIST_TRX_{0}".format(username))

print("Incremental Report:")
incReadDf.show()


#-----------------------------------------------------
#               JOIN INCREMENTAL READ WITH CUST INFO
#-----------------------------------------------------

### LOAD CUSTOMER DATA REFINED
custDf = spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.CUST_TABLE_REFINED_{0}".format(username))

print("Cust DF Schema: ")
custDf.printSchema()

joinDf = incReadDf.join(custDf, custDf.CREDIT_CARD_NUMBER == incReadDf.credit_card_number, 'inner')

distanceFunc = F.udf(lambda arr: (((arr[2]-arr[0])**2)+((arr[3]-arr[1])**2)**(1/2)), FloatType())
distanceDf = joinDf.withColumn("trx_dist_from_home", distanceFunc(F.array("latitude", "longitude",
                                                                            "address_latitude", "address_longitude")))

# SELECT CUSTOMERS WHERE TRANSACTION OCCURRED MORE THAN 100 MILES FROM HOME
distanceDf = distanceDf.filter(distanceDf.trx_dist_from_home > 50)


#---------------------------------------------------
#               SAVE DATA TO NEW ICEBERG TABLE
#---------------------------------------------------

#distanceDf.show()

gold_cols = ['transaction_amount', 'transaction_currency', 'transaction_type', 'trx_dist_from_home', 'name', 'email', 'bank_country', 'account_no']

distanceDf.select(*gold_cols).writeTo("spark_catalog.HOL_DB_{0}.GOLD_TABLE_{0}".format(username)).using("iceberg").createOrReplace()

#spark.sql("SELECT * FROM spark_catalog.HOL_DB_{0}.GOLD_TABLE_{0}".format(username)).show()
