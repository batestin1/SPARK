#! /usr/bin/env bash

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Banco Braavos
#     Start Bash
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
echo "START PROJECT"
cd ..

cd source

rm -r account
rm -r accountcard
rm -r address
rm -r card 
rm -r client
rm -r rent 
cd ..
cd source
mkdir account
mkdir accountcard
mkdir address
mkdir card
mkdir client
mkdir rent

cd ..

cd script

python create_dataset.py 
python insert_bronze.py 
python insert_silver.py 
python insert_gold.py