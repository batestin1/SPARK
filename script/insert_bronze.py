#!/usr/local/bin/python3

#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Banco Braavos
#     Repositorio: Bronze
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

print("Starting transaction for Bronze file...")

###################account########################################

acc = spark.read.option("multiline", "True").json("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/source/account/")
acc.write.mode("overwrite").format("parquet").partitionBy("_yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/account/parquet/")
acc.write.mode("overwrite").format("orc").partitionBy("_yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/account/orc/")

###################accountcard########################################
accard = spark.read.option("multiline", "True").json("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/source/accountcard/")
accard.write.mode("overwrite").format("parquet").partitionBy("_yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/accountcard/parquet/")
accard.write.mode("overwrite").format("orc").partitionBy("_yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/accountcard/orc/")

###################address########################################
address = spark.read.option("multiline", "True").json("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/source/address/")
address.write.mode("overwrite").format("parquet").partitionBy("_yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/address/parquet/")
address.write.mode("overwrite").format("orc").partitionBy("_yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/address/orc/")

###################card########################################
card = spark.read.option("multiline", "True").json("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/source/card/")
card.write.mode("overwrite").format("parquet").partitionBy("_yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/card/parquet/")
card.write.mode("overwrite").format("orc").partitionBy("_yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/card/orc/")

###################client########################################
client = spark.read.option("multiline", "True").json("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/source/client/")
client.write.mode("overwrite").format("parquet").partitionBy("_yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/client/parquet/")
client.write.mode("overwrite").format("orc").partitionBy("_yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/client/orc/")

###################rent########################################
rent = spark.read.option("multiline", "True").json("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/source/rent/")
rent.write.mode("overwrite").format("parquet").partitionBy("_yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/rent/parquet/")
rent.write.mode("overwrite").format("orc").partitionBy("_yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/rent/orc/")