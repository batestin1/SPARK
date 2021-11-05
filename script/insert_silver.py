#!/usr/local/bin/python3

#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Banco Braavos
#     Repositorio: Silver
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

print("Starting transaction for silver file...")

###################client########################################

#read
client = spark.read.orc("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/client/orc/").createOrReplaceTempView("client")
account = spark.read.orc("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/account/orc/").createOrReplaceTempView("account")
rent = spark.read.orc("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/rent/orc/").createOrReplaceTempView("rent")
address = spark.read.orc("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/address/orc/").createOrReplaceTempView("address")
account_card = spark.read.orc("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/accountcard/orc/").createOrReplaceTempView("accountcard")
card = spark.read.orc("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/bronze/card/orc/").createOrReplaceTempView("card")

#deduplication
client = spark.sql(""" SELECT l. * FROM (SELECT id_client, itin, name, family, house, words, title, gender, date_birth, faith, god_to_pray, yearmonthday, row_number() over (partition by id_client order by yearmonthday desc) as row_id FROM client WHERE TRIM(id_client) <> '') l WHERE row_id = 1""")
account = spark.sql(" SELECT l. * FROM (SELECT *, row_number() over ( partition by id_client, account_type, bban_count order by yearmonthday desc) as row_id FROM account WHERE TRIM(account_status) not in('not activated')) l WHERE row_id = 1")
rent = spark.sql("SELECT l.* FROM (SELECT *, row_number() over (partition by id_client order by yearmonthday desc) as row_id FROM rent) l WHERE l.row_id = 1")
address = spark.sql("SELECT l. * FROM (SELECT *, row_number() over (partition by id_client order by yearmonthday desc) as row_id from address) l WHERE l.row_id = 1 ")
accountcard = spark.sql("SELECT l. * FROM (SELECT *, row_number() over (partition by id_client order by yearmonthday desc) as row_id from accountcard WHERE TRIM(card_number) not in('not activated')) l WHERE l.row_id = 1 ")
card = spark.sql("SELECT l. * FROM (SELECT *, row_number() over (partition by id_client order by yearmonthday desc) as row_id from card) l WHERE l.row_id = 1 ")

#load
client.write.mode("overwrite").format("orc").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/client/orc/")
client.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/client/parquet/")
account.write.mode("overwrite").format("orc").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/account/orc/")
account.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/account/parquet/")
rent.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/rent/parquet/")
rent.write.mode("overwrite").format("orc").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/rent/orc/")
address.write.mode("overwrite").format("orc").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/address/orc/")
address.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/address/parquet/")
accountcard.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/accountcard/parquet/")
accountcard.write.mode("overwrite").format("orc").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/accountcard/orc/")
card.write.mode("overwrite").format("orc").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/card/orc/")
card.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/card/parquet/")
