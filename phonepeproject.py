!git clone https://github.com/PhonePe/pulse.git

import pandas as pd
import json
import os
#This is to direct the path to get the data as states


path="pulse/data/aggregated/transaction/country/india/state/"
Agg_state_list=os.listdir(path)
Agg_state_list
#Agg_state_list--> to get the list of states in India

#<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

#This is to extract the data's to create a dataframe

clm={'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}
for i in Agg_state_list:
    p_i=path+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              clm['Transacion_type'].append(Name)
              clm['Transacion_count'].append(count)
              clm['Transacion_amount'].append(amount)
              clm['State'].append(i)
              clm['Year'].append(j)
              clm['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
Agg_Trans=pd.DataFrame(clm)
Agg_Trans
#This is to direct the path to get the data as states


path = "pulse/data/aggregated/user/country/india/state/"
Agg_state_list = os.listdir(path)
clm = {'State': [], 'Year': [], 'Quater': [], 'brand': [], 'Device_count': [], 'percentage': []}

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

            # Check if usersByDevice exists and is not None
            if D["data"] is not None and D["data"]["usersByDevice"] is not None:
                for z in D["data"]["usersByDevice"]:
                    brand = z["brand"]
                    count = z["count"]
                    per = z["percentage"]
                    clm['brand'].append(brand)
                    clm['Device_count'].append(count)
                    clm['percentage'].append(per)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))

# Create the DataFrame
Agg_User = pd.DataFrame(clm)
Agg_User
path="pulse/data/map/transaction/hover/country/india/state/"
Map_state_list=os.listdir(path)
Map_state_list

clm={'State':[], 'Year':[],'Quater':[],'Area':[], 'Transacion_count':[], 'Transacion_amount':[]}
for i in Map_state_list:
    p_i=path+i+"/"
    Map_yr=os.listdir(p_i)
    for j in Map_yr:
        p_j=p_i+j+"/"
        Map_yr_list=os.listdir(p_j)
        for k in Map_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['hoverDataList']:
              Name=z['name']
              count=z['metric'][0]['count']
              amount=z['metric'][0]['amount']
              clm['Area'].append(Name)
              clm['Transacion_count'].append(count)
              clm['Transacion_amount'].append(amount)
              clm['State'].append(i)
              clm['Year'].append(j)
              clm['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
Map_Trans=pd.DataFrame(clm)
Map_Trans
path = "pulse/data/map/user/hover/country/india/state/"
Map_state_list = os.listdir(path)

clm = {'State': [], 'Year': [], 'Quater': [], 'Area': [], 'registeredUsers': [], 'appOpens': []}

for state in Map_state_list:
    state_path = os.path.join(path, state)
    Map_year_list = os.listdir(state_path)

    for year in Map_year_list:
        year_path = os.path.join(state_path, year)
        Map_quarter_list = os.listdir(year_path)

        for quarter in Map_quarter_list[:10]:
            quarter_path = os.path.join(year_path, quarter)

            with open(quarter_path, 'r') as file:
                data = json.load(file)
                hover_data = data['data']['hoverData']

                for district, district_data in hover_data.items():
                    registered_users = district_data['registeredUsers']
                    app_opens = district_data['appOpens']

                    clm['Area'].append(district)
                    clm['registeredUsers'].append(registered_users)
                    clm['appOpens'].append(app_opens)
                    clm['State'].append(state)
                    clm['Year'].append(year)
                    clm['Quater'].append(int(quarter.strip('.json')))

# Create a DataFrame
Map_User = pd.DataFrame(clm)
Map_User
path = "pulse/data/top/transaction/country/india/state/"
Top_state_list = os.listdir(path)

clm = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Pincode': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in Top_state_list:
    p_i = path + i + "/"
    Top_year = os.listdir(p_i)
    for j in Top_year:
        p_j = p_i + j + "/"
        Top_year_list = os.listdir(p_j)
        for k in Top_year_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            D = json.load(Data)
            for district in D['data']['districts']:
                district_name = district["entityName"]
                district_count = district['metric']['count']
                district_amount = district['metric']['amount']
                for pincode in D['data']['pincodes']:
                    pincode_name = pincode["entityName"]
                    pincode_count = pincode['metric']['count']
                    pincode_amount = pincode['metric']['amount']
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip('.json')))
                    clm['District'].append(district_name)
                    clm['Pincode'].append(pincode_name)
                    clm['Transaction_count'].append(pincode_count)
                    clm['Transaction_amount'].append(pincode_amount)

# Successfully created a DataFrame
Top_Trans = pd.DataFrame(clm)
Top_Trans

path = "pulse/data/top/user/country/india/state/"
Top_state_list = os.listdir(path)

clm = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Pincode': [], 'User': []}

for i in Top_state_list:
    p_i = path + i + "/"
    Top_year = os.listdir(p_i)
    for j in Top_year:
        p_j = p_i + j + "/"
        Top_year_list = os.listdir(p_j)
        for k in Top_year_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            D = json.load(Data)
            for district in D['data']['districts']:
                district_name = district['name']
                district_users = district['registeredUsers']
                for pincode in D['data']['pincodes']:
                    pincode_name = pincode['name']
                    pincode_users = pincode['registeredUsers']
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip('.json')))
                    clm['District'].append(district_name)
                    clm['Pincode'].append(pincode_name)
                    clm['User'].append(pincode_users)

# Successfully created a DataFrame
Top_User = pd.DataFrame(clm)
Top_User
