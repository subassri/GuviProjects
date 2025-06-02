import os
import pandas as pd
import json

# Define the path
path = "data/aggregated/transaction/country/india/state/"


# Get the list of states
Agg_state_list = os.listdir(path)

clm = {'State': [], 'Year': [], 'Quater': [], 'Transacion_type': [], 'Transacion_count': [], 'Transacion_amount': []}

for i in Agg_state_list:
    p_i = path + i + "/"
    Agg_yr = os.listdir(p_i)
    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)
        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            D = json.load(Data)
            for z in D['data']['transactionData']:
                Name = z['name']
                count = z['paymentInstruments'][0]['count']
                amount = z['paymentInstruments'][0]['amount']
                clm['Transacion_type'].append(Name)
                clm['Transacion_count'].append(count)
                clm['Transacion_amount'].append(amount)
                clm['State'].append(i)
                clm['Year'].append(j)
                clm['Quater'].append(int(k.strip('.json')))

Agg_Trans = pd.DataFrame(clm)
print(Agg_Trans)
