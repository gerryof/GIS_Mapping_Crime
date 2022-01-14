# -*- coding: utf-8 -*-
"""
Created on Wed Dec  8 11:41:58 2021

@author: Data
"""

import geopandas
import numpy as np
import pandas as pd
from shapely.geometry import Point

import missingno as msn

import seaborn as sns
import matplotlib.pyplot as plt

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

zipfile = r"E:\GIS Mapping\data\Census2011_Garda_SubDistricts_Nov2013.zip"

areas = geopandas.read_file(zipfile)

# below brings in the crime data and create the join between the two datasets

crimedata = r"E:\Api_Extraction\data\crimedata.csv"

crime = pd.read_csv(crimedata)

crimestations = pd.Series(crime['Garda Station'].unique())

crimestations = crimestations.str.replace('Division' , '')

crimestations = crimestations.str.split(' ' , n=1, expand =True)

crimestations.columns = ['st_num' , 'mixed']

division = pd.Series(crimestations['mixed']).str.split(',' , n =1 , expand = True)

division.columns = ['station' , 'division']

crimestations = pd.merge(crimestations, division ,left_index=True , right_index=True)

crimestations = crimestations.drop(['mixed', 'division'] , axis=1)

crimestations['Garda Station'] = pd.Series(crime['Garda Station'].unique())


# below is the bits and pieces to try and patch the disperate data sources together.

areas = areas[(areas.DIVISION == 'Galway')]

mapper = areas[['SUB_DIST' ,  'SUB_CODE', 'SUB_IRISH']].drop_duplicates()

mapper = mapper.merge(crimestations , how = 'left' , left_on = 'SUB_DIST' , right_on = 'station' )# duplication

mapper = mapper.merge(crimestations , how = 'left' , left_on = 'SUB_IRISH' , right_on = 'station' )

mapper.st_num_x.fillna( mapper['st_num_y'] , inplace=True)

mapper.station_x.fillna( mapper['station_y'] , inplace=True)

mapper = mapper.drop(['st_num_y', 'station_y'], axis=1)

# creating a fuzzy matcher for the remaining 11 unmatched stations
fuz_a = pd.DataFrame(mapper[(mapper.st_num_x.isnull())].SUB_IRISH.drop_duplicates())

fuz_b = pd.DataFrame(crimestations.station)

fuzzyness = fuz_a.merge(fuz_b ,how='cross')

#fuzzyness['fuzzy_partial'] = 

fuzzy_array = np.array(fuzzyness)

fuzzy_list = []

for a, b in fuzzy_array:
    part = fuzz.partial_ratio(a , b)
    ratio = fuzz.ratio(a , b)
    token = fuzz.token_sort_ratio(a , b)
    arr = [part ,ratio , token]
    fuzzy_list.append(arr)

stack = np.column_stack((fuzzy_array , fuzzy_list))
  
fuzzy_results = pd.DataFrame (stack)

fuzzy_results.columns = ['mapper_st' , 'crime_st' , 'par_ratio' , 'ratio' , 'token']

fuzzy_results_gp = fuzzy_results.groupby(['mapper_st'] , as_index=False)[ 'ratio' , 'token'].apply(lambda x : x.astype(int).max())

fuzzy_results_gp['highest'] = fuzzy_results_gp.max(axis=1)

con_dict = {'mapper_st' : str, 'crime_st' : str , 'par_ratio' : str , 'ratio' : int , 'token' : int} 

fuzzy_results = fuzzy_results.astype(con_dict)

fuzzy_results['highest'] = fuzzy_results.max(axis=1, numeric_only=True)

fuzzy_results_gp = fuzzy_results_gp[(fuzzy_results_gp.highest >= 80)]

fuzzy_results_hi = fuzzy_results.merge(fuzzy_results_gp , how='inner',  left_on=['mapper_st', 'highest'], right_on=['mapper_st' , 'highest'])
    
mapper_fin = mapper.merge(fuzzy_results_hi , how='left', left_on = 'SUB_IRISH' , right_on='mapper_st')

mapper_fin = mapper_fin.merge(crimestations , how='left' , left_on='crime_st' , right_on='station')

mapper_fin['Garda Station_x'].fillna(mapper_fin['Garda Station'] , inplace=True)

mapper_fin['Garda Station_x'].fillna(mapper_fin['Garda Station_y'] , inplace=True)

mapper_fin = mapper_fin[['SUB_CODE' , 'Garda Station_x']].dropna()

mapper_fin.columns = ['SUB_CODE', 'Garda_Station']

mapper_fin.to_csv('./data/area_crime_mapper.csv')  


