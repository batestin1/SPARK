#!/usr/local/bin/python3

#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Banco Braavos
#     Repositorio: Gold
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import json
from pyspark.sql import SparkSession
import mysql.connector
from sqlalchemy import create_engine

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

print("Starting transaction for gold file...")

###################client########################################

#read
client = spark.read.orc("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/client/orc/").createOrReplaceTempView("client")
account = spark.read.orc("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/account/orc/").createOrReplaceTempView("account")
rent = spark.read.orc("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/rent/orc/").createOrReplaceTempView("rent")
address = spark.read.orc("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/address/orc/").createOrReplaceTempView("address")
accountcard = spark.read.orc("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/accountcard/orc/").createOrReplaceTempView("accountcard")
card = spark.read.orc("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/silver/card/orc/").createOrReplaceTempView("card")



#deduplication
client = spark.sql("""
SELECT id_client, itin, name, family, house, words, title, gender, date_birth, faith, god_to_pray, yearmonthday
FROM client""")
account = spark.sql("""
SELECT id_client,bank,account_type,investor_profile, account_status, bban_count,aba
FROM account
""")
rent = spark.sql("""
SELECT id_client,rent_income_value, description_chosen_income_method,
total_account_value,current_account_total_value, total_amount_carried_over
FROM rent
                            
""")

address = spark.sql("""
SELECT id_client, postalcode, street, number, birth_culture, current_culture, country_of_birth, birth_region, city_of_birth,currency_city
FROM address
""")

accountcard = spark.sql("""
SELECT id_client, status_account_blocked, card_number, transaction_code, date_transaction, time_transaction, describle_transaction,
original_transaction_amount, number_installments_assign, current_installment, currency
FROM accountcard

""")
card = spark.sql("""
SELECT id_client, total_limit_value, limit_value_available, total_limit_used, description_card, security_card, expire_card_number
FROM card
""")

#transform
client_full_address= spark.sql("""SELECT 
CL.id_client, CL.itin, CL.name as first_name, CL.family, CL.house, CL.words,CL.title, CL.gender, CL.date_birth, CL.faith, CL.god_to_pray,CL.yearmonthday,
CT.id_client as id_address, CT.postalcode, CT.street, CT.number, CT.birth_culture, CT.country_of_birth, CT.city_of_birth, CT.current_culture, CT.currency_city
FROM client CL INNER JOIN address CT ON LPAD(CL.id_client, 20, '0') = LPAD(CT.id_client, 20, '0')
""").createOrReplaceTempView("client_full_address")

client_full_rent = spark.sql("""SELECT
CL.id_client, CL.itin, CL.name as first_name, CL.family, CL.house, CL.words,CL.title, CL.gender, CL.date_birth, CL.faith, CL.god_to_pray,
RT.id_client as id_rent, RT.rent_income_value, RT.description_chosen_income_method, RT.total_account_value,current_account_total_value, RT.total_amount_carried_over
FROM client CL INNER JOIN rent RT ON LPAD(CL.id_client, 20, '0') = LPAD(RT.id_client, 20, '0')
""").createOrReplaceTempView("client_full_rent")

account_full_card = spark.sql("""SELECT
AC.id_client, AC.bank,account_type, AC.investor_profile, AC.account_status, AC.bban_count, AC.aba,
CA.id_client as id_card, CA.total_limit_value, CA.limit_value_available,  CA.total_limit_used, CA.description_card, CA.security_card, CA.expire_card_number
FROM account AC INNER JOIN card CA ON LPAD(AC.id_client, 20, '0') = LPAD(CA.id_client, 20, '0')
""").createOrReplaceTempView("account_full_card")

account_full_accountcard = spark.sql("""SELECT
AC.id_client, AC.bank,account_type, AC.investor_profile, AC.account_status, AC.bban_count, AC.aba,
ACC.id_client as id_accountcard, ACC.status_account_blocked, ACC.transaction_code, ACC.date_transaction, ACC.time_transaction, ACC.describle_transaction,ACC.original_transaction_amount, ACC.number_installments_assign,ACC.current_installment, ACC.currency
FROM account AC INNER JOIN accountcard ACC ON LPAD(AC.id_client, 20, '0') = LPAD(ACC.id_client, 20, '0')

""").createOrReplaceTempView("account_full_accountcard")


client_full_add_rent = spark.sql("""SELECT
CT.id_client, CT.itin, CT.first_name, CT.family, CT.house, CT.words, CT.title, CT.gender,CT.date_birth, CT.faith, CT.god_to_pray, CT.postalcode, CT.street, CT.number, CT.birth_culture, CT.country_of_birth, CT.city_of_birth, CT.current_culture, CT.currency_city, CT.yearmonthday,
RT.id_client as id_crent,RT.rent_income_value,RT.description_chosen_income_method,RT.total_account_value,RT.current_account_total_value,RT.total_amount_carried_over
FROM client_full_address CT INNER JOIN client_full_rent RT ON LPAD(CT.id_client, 20, '0') = LPAD(RT.id_client, 20, '0')
""").createOrReplaceTempView("client_full_add_rent")

account_full_cards = spark.sql("""SELECT
AC.id_client , AC.bank , AC.account_type ,AC.investor_profile , AC.account_status , AC.bban_count , AC.aba , AC.total_limit_value, AC.limit_value_available , AC.total_limit_used , AC.description_card , AC.security_card , AC.expire_card_number,
ACC.status_account_blocked,  ACC.transaction_code,  ACC.date_transaction,  ACC.time_transaction,  ACC.describle_transaction,  ACC.original_transaction_amount, ACC.number_installments_assign, ACC.current_installment, ACC.currency
FROM account_full_card AC INNER JOIN account_full_accountcard ACC ON LPAD(AC.id_client, 20, '0') = LPAD(ACC.id_client, 20, '0') 
""").createOrReplaceTempView("account_full_cards")


client_full_account = spark.sql("""
SELECT * FROM  client_full_add_rent CF INNER JOIN account_full_cards AC ON LPAD(CF.id_client, 20, '0') = LPAD(AC.id_client, 20, '0')
""").createOrReplaceTempView("client_full_account")

client_full = spark.sql("""SELECT
first_name,itin,family,house,words,title,gender,date_birth,faith,god_to_pray,postalcode,street,number,birth_culture,country_of_birth,city_of_birth,current_culture,currency_city,rent_income_value,description_chosen_income_method,total_account_value,current_account_total_value,total_amount_carried_over,bank,account_type,investor_profile,account_status,bban_count,aba,total_limit_value, limit_value_available,total_limit_used,description_card,security_card,expire_card_number,status_account_blocked,transaction_code,date_transaction,time_transaction,describle_transaction,original_transaction_amount,number_installments_assign,current_installment,currency,yearmonthday
FROM client_full_account
""")

#load
#parquet
client_full.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/gold/client_of_iron_bank/parquet/")
#csv
client_full.write.option("header", True).csv("C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/gold/client_of_iron_bank/excel/client_of_iron_bank.csv")
#mysql
#connection
bank = mysql.connector.connect(
    host = "localhost",
    user= "root",
    password = ""
)

cursor = bank.cursor()
cursor.execute('CREATE DATABASE ironbank')
my_conn = create_engine('mysql+mysqldb://root:@localhost/ironbank')
tab1 = client_full.toPandas()
tab1.to_sql(con=my_conn, name='iron_bank_accounts', if_exists='append', index=False).encode("utf-8")

