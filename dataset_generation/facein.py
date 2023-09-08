import names
import random
import requests
import pandas as pd
# fetch nationalities and country code 
urlcountries = "https://raw.githubusercontent.com/Imagin-io/country-nationality-list/master/countries.json"
responsecountries=requests.get(urlcountries)
country_data=responsecountries.json()
country_names = [{'nationality': country['nationality'], 'num_code': country['num_code']} for country in country_data]
# create dataframe FaceIn
FaceIn = pd.DataFrame(country_names)
FaceIn =  FaceIn[FaceIn['num_code'].astype(int)<=50]
#Add ID
FaceIn['ID']=range(1,len(FaceIn)+1)
#Add Hobby 
urlhobby="https://gist.githubusercontent.com/carlelieser/884584d06b2d9429f321ec192f6dc7b5/raw/0888b5449ecda4787001b74811e645d0a74b8132/hobbies.json"
responsehobby=requests.get(urlhobby)
hobby_data=responsehobby.json()
FaceIn['Hobby']= [hobby['title'] for hobby in hobby_data[:len(FaceIn)]]
FaceIn.head(10)

FaceIn.to_csv('FaceIn',sep=',')
