#!/usr/local/bin/python3
#coding: utf-8
#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Banco Braavos
#     Repositorio: Origem
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import json
import csv
from faker import Faker
import random
from datetime import date, datetime

#setup
faker = Faker()



val = int(input("Enter the number of records you want to generate: "))
# create a dataset of json in source files
print("Starting the simulation...")
num = 0
for x in range(val):
    num = num + 1
    
    with open(f"C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/source/client/cl_{num}.json", "w", encoding='utf-8') as out1:
          with open(f"C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/source/address/ad_{num}.json", "w", encoding='utf-8') as out2:
            with open(f"C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/source/account/ac_{num}.json", "w", encoding='utf-8') as out3:
              with open(f"C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/source/accountcard/accard_{num}.json", "w", encoding='utf-8') as out4:
                with open(f"C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/source/card/card_{num}.json", "w", encoding='utf-8') as out5:
                   with open(f"C:/Users/Bates/Documents/Repositorios/SPARK/IronBankBraavos/source/rent/rent_{num}.json", "w", encoding='utf-8') as out8:
                          ##################### variables ##############################
                          female_name = faker.first_name_female()
                          male_name = faker.first_name_male()
                          gend_name_dict = random.choice([{"Female": female_name}, {"Male":male_name}])
                          name_resume = list(gend_name_dict.values())[0]
                          gender_resume = list(gend_name_dict.keys())[0]
                          lastname = faker.last_name()
                          fullname = f"""{name_resume} {lastname}"""
                          title = f"""{faker.prefix()} {name_resume} of house {lastname}"""
                          family = lastname
                          house = lastname
                          nb = random.randint(1,10)
                          slogan = faker.sentence(nb_words=nb, variable_nb_words=False)
                          id_client = faker.bban()
                          cultureBorn = random.choice(["Northmen", "Braavosi", "Free Folk", "Andalos", "Dothraki", "Roinar", "iron men", "Valyrian", "ghiscari", "First Men", "Westeros", "Unknown", "Uninformed"])
                          cultureLiving = random.choice(["Northmen", "Braavosi", "Free Folk", "Andalos", "Dothraki", "Roinar", "iron men", "Valyrian", "ghiscari", "First Men", "Westeros", "Unknown", "Uninformed"])
                          continetalBorn = random.choice([{"Westeros": ("North of the Wall ", "The North ", "The Riverlands", "The Vale", "The Iron Islands", "The Westerlands", "The Reach", "The Crownlands", "The Stormlands", "Dorne")}, 
                          {"Essos": ("Free Cities", "Slave's Bay", "Dothraki Sea",  "Red Waste", "Valyrian peninsula", "Other")},
                          {"Sothoryos":("Naath", "Isle of Tears", "Basilisk Point")}])
                          continetalBornKey = list(continetalBorn.keys())[0]
                          continetalBornVal = random.choice(list(continetalBorn.values()))
                          continetalBornValChoice = random.choice(continetalBornVal)
                          continetalLiving = random.choice([{"Westeros": ("North of the Wall ", "The North ", "The Riverlands", "The Vale", "The Iron Islands", "The Westerlands", "The Reach", "The Crownlands", "The Stormlands", "Dorne")}, 
                          {"Essos": ("Free Cities", "Slave's Bay", "Dothraki Sea",  "Red Waste", "Valyrian peninsula", "Other")},
                          {"Sothoryos":("Naath", "Isle of Tears", "Basilisk Point")}])
                          continetalLivingKey = list(continetalLiving.keys())[0]
                          continetalLivingVal = random.choice(list(continetalLiving.values()))
                          continetalLivingValChoice = random.choice(continetalLivingVal)
                          currentCity = f"""{random.choice(["Braavos", "Lorath‎", "Lys‎", "Magisters‎", "Myr‎", "Norvos‎", "Pentos‎", "Qohor‎", "Tyrosh‎", "Volantis","Unknown", "Uninformed", "Asshai‎", "Astapor", "New Ghis‎", "Asabhad", "Bayasabhad", "Carcosa", "Cities of the Bloodless Men", "City of the Winged Men", "Ebonhead", "Elyria", "Faros", "Gulltown", "Hesh", "Ib Nor", "Ib Sar", "Jinqi", "K'Dath", "Kayakayanaya", "King's Landing", "Kosrak", "Lannisport", "Leng Ma", "Leng Yi", "Lhazosh", "Lotus Point", "Mantarys‎", "Meereen", "Oldtown", "Qarth‎", "Vaes Dothrak‎", "White Harbor‎", "Yunkai‎", "Winterfell", "Unknown", "Uninformed"])}"""
                          bornCity = f"""{random.choice(["Braavos‎", "Lorath‎", "Lys‎", "Magisters‎", "Myr‎", "Norvos‎", "Pentos‎", "Qohor‎", "Tyrosh‎", "Volantis","Unknown", "Uninformed", "Asshai‎", "Astapor", "New Ghis‎", "Asabhad", "Bayasabhad", "Carcosa", "Cities of the Bloodless Men", "City of the Winged Men", "Ebonhead", "Elyria", "Faros", "Gulltown", "Hesh", "Ib Nor", "Ib Sar", "Jinqi", "K'Dath", "Kayakayanaya", "King's Landing", "Kosrak", "Lannisport", "Leng Ma", "Leng Yi", "Lhazosh", "Lotus Point", "Mantarys‎", "Meereen", "Oldtown", "Qarth‎", "Vaes Dothrak‎", "White Harbor‎", "Yunkai‎", "Winterfell", "Unknown", "Uninformed"])}"""
                          itin = faker.ssn()
                          date = f"{faker.date_of_birth()}"
                          gender = gender_resume
                          future = faker.future_date()
                          yearmonthday = f"""{future.strftime('%Y%m%d')}"""
                          valor_positivo = f"+{float(random.randint(1,999999))}"
                          valor_negativo = f"-{float(random.randint(1,999999))}"
                          valor_positivo2 = f"+{float(random.randint(1,999999))}"
                          valor_negativo2 = f"-{float(random.randint(1,999999))}"
                          valorliquido = random.choice([valor_positivo,valor_negativo])
                          postalcode = f"""{faker.postcode()}"""
                          street = faker.street_name()
                          number_street = f"""{faker.building_number()}"""                          
                          descricao_metodo_renda_eleita = f"{faker.bban()[:2]}"
                          valor_bruto_renda_eleita = random.choice([valor_positivo2,valor_negativo2])
                          card_number = f"""{faker.credit_card_number()}"""
                          status_bloqueio_conta = random.choice(["activated", "not activated"])
                          v0 = float(random.randint(1,999999))
                          v1 = float(random.randint(1,99999))
                          v2 = float(random.randint(1,9999))
                          valor_limite_total = f"""{v0}"""
                          valor_limite_utilizado = f"{v1}"
                          valor_limite = v0 - v1 
                          valor_limite_disponivel = f"{valor_limite}"
                          total_limit_used = f"{v0 - valor_limite}"
                          descricao_produto = faker.credit_card_provider()
                          religion = random.choice(["other", "Uninformed", "old gods", "Faith of the Seven", "Drowned God", "Many-Faced God", "Dothraki Religion", "Gardens of Gelenei"]).upper()
                          gods = random.choice(["other", "Uninformed","Sun", "Moon",  "Moonsingers", "Fountain of the Drunken God", "R'hllor", "Great Other", "Mother Rhoyne", "Aquan the Red Bull", "Bakkalon", "Black Goat", "Great Shepherd", "Hooded Wayfarer", "horse god","Lady of Spears", "Lion of Night",  "Merling King", "Moon-Pale Maiden", "Pattern", "Semosh and Selloso", "Silent God", "Stone Cow of Faros", "Father of Waters", "Weeping Lady of Lys", "Pantera", "Yndros of the Twilight", "Saagael", "Maiden-Made-of-Light", "Cult of Starry Wisdom", "Moon Mother", "Mother Rhoyne"]).upper()
                          security_card = f"{faker.credit_card_security_code()}"
                          expire_card_number = f"{faker.credit_card_expire()}"
                          transaction_code = f"{faker.bban()}"
                          future_transaction = f"{faker.future_date()}"
                          date_transaction = f"""{future.strftime('%Y%m%d')}"""
                          time_transaction = f"{faker.time()}"
                          describle_transaction = random.choice(["Dragons","brothel", "golden company", "Marketplace", "witchcraft", "horses", "wolfs", "Loan to Finance War", "Tribute to the Gods", "immaculate army", "slaves", "ships", "swords", "royal forgiveness", "hill wine", "dormant wine", "winter clothing", "fishing vessel", "Iron Island Men's Fleet", "theater", "dothraki army", "savage army", "night patrol army", "Royal Guard army", "army of the dead", "Giants Army", "horn of Joramun"])
                          original_transaction_amount = f"{float(random.randint(1,999999))}"
                          number = random.randint(1,9)
                          number_installments_assign = f"{number}"
                          current_installment = f"{number - 1}"
                          currency = f"{faker.currency_code()} of Braavos"
                          account_type = random.choice(["checking account", "savings account", "salary bill", "salary bill", "dragon account", "account for the long winter"])
                          investor_profile = random.choice(["Reliable", "Vicious", "Risky", "Crazy", "Temporary"])
                          bban_count = faker.bban()
                          aba = f"0{random.randint(11111111,99999999)}"
                          v01 = float(random.randint(1,9999999))
                          v11 = float(random.randint(1,999999))
                          v22 = float(random.randint(1,99990))
                          total_account_value =f"{v01}"
                          current_account_total_value = f"{v01-v11}"
                          total_amount_carried_over = f"{v11}"
                          account_status = random.choice(["activated", "not activated"])




                          ##################### df client ##############################
                          client = {
                            "id_client": id_client,
                            "itin": itin,
                            "name": fullname,
                            "family": lastname,
                            "house": house,
                            "words": slogan,
                            "title": title,
                            "gender": gender,
                            "date_birth": date,
                            "faith": religion, 
                            "god_to_pray": gods,
                            "yearmonthday": yearmonthday,
                            "_yearmonthday":yearmonthday

                          }
                          json.dump(client, out1, indent=True, separators=(',',':'),ensure_ascii=False)
                          
                          ##################### df address ##############################

                          address = {
                            "id_client": id_client,
                            "postalcode":  postalcode,
                            "street": street,
                            "number": number_street,       
                            "birth_culture": cultureBorn,
                            "current_culture": cultureLiving,
                            "country_of_birth": continetalBornKey,
                            "birth_region": continetalBornValChoice,
                            "city_of_birth": bornCity,
                            "currency_city": currentCity,
                            "yearmonthday": yearmonthday,
                            "_yearmonthday":yearmonthday

                          }
                          json.dump(address, out2, indent=True, separators=(',',':'),ensure_ascii=False)
                          
                          ##################### df account ##############################

                          account = {
                            "id_client": id_client,
                            "yearmonthday": yearmonthday,
                            "_yearmonthday":yearmonthday,
                            "bank": "Braavos",
                            "account_type": account_type,
                            "investor_profile": investor_profile,
                            "account_status": account_status,
                            "bban_count": bban_count,
                            "aba": aba                         


                          }
                          json.dump(account, out3, indent=True, separators=(',',':'),ensure_ascii=False)
                          
                          ##################### df accountcard ##############################

                          accountcard = {
                            "id_client": id_client,
                            "yearmonthday": yearmonthday,
                            "_yearmonthday":yearmonthday,
                            "status_account_blocked": status_bloqueio_conta,
                            "card_number": card_number,
                            "transaction_code": transaction_code,
                            "date_transaction": future_transaction,
                            "time_transaction": time_transaction,
                            "describle_transaction": describle_transaction,
                            "original_transaction_amount": original_transaction_amount,
                            "number_installments_assign": number_installments_assign,
                            "current_installment": current_installment,
                            "currency": currency                          


                          }
                          json.dump(accountcard, out4, indent=True, separators=(',',':'), ensure_ascii=False)

                          ##################### df card ##############################

                          card = {
                            "id_client": id_client,
                            "yearmonthday": yearmonthday,
                            "_yearmonthday":yearmonthday,
                            "card_number": status_bloqueio_conta,
                            "total_limit_value": valor_limite_total,
                            "limit_value_available": valor_limite_disponivel,
                            "total_limit_used": total_limit_used,
                            "description_card": descricao_produto,
                            "security_card": security_card,
                            "expire_card_number": expire_card_number
                          }
                          json.dump(card, out5, indent=True, separators=(',',':'),ensure_ascii=False)
                          
                          
                          ##################### df rent ##############################

                          rent = {
                            "id_client": id_client,
                            "yearmonthday": yearmonthday,
                            "_yearmonthday":yearmonthday,
                            "rent_income_value": valorliquido,
                            "description_chosen_income_method": valor_bruto_renda_eleita,
                            "total_account_value": total_account_value,
                            "current_account_total_value": current_account_total_value,
                            "total_amount_carried_over": total_amount_carried_over
                            
                          }
                          json.dump(rent, out8, indent=True, separators=(',',':'),ensure_ascii=False)
                          
    
