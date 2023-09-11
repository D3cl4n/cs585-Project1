import names
import random
import requests
import pandas as pd

class FaceInData:
    def __init__(self, size):
        self.size = size

    def generateData(self):
        # fetch nationalities and country code 
        urlcountries = "https://raw.githubusercontent.com/Imagin-io/country-nationality-list/master/countries.json"
        responsecountries=requests.get(urlcountries)
        country_data=responsecountries.json()
        country_names = [country['nationality'] for country in country_data]
        #Add Hobby 
        urlhobby="https://gist.githubusercontent.com/carlelieser/884584d06b2d9429f321ec192f6dc7b5/raw/0888b5449ecda4787001b74811e645d0a74b8132/hobbies.json"
        responsehobby=requests.get(urlhobby)
        hobby_data=responsehobby.json()
        hobbies=[hobby['title'] for hobby in hobby_data]
        # create dataframe FaceIn
        FaceIn=[]

        for i in range(1,10):
        #     rand_index=random.randint(0,len(country_names)-1)
            rand_index=random.randint(1,50)
            FaceIn.append([i,names.get_full_name(),country_names[rand_index],rand_index,random.choice(hobbies)])
    
        FaceIn=pd.DataFrame(FaceIn,columns=['ID','Name','Nationaliy','Country Code','Hobby'])
        # FaceIn   
    
        FaceIn.to_csv('FaceIn.csv',sep=',',index=False)
