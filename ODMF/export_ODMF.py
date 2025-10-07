# -*- coding: utf-8 -*-
"""
Created on Mon Oct  6 16:25:49 2025

@author: garre
"""

from odmfclient import login

with login("https://data.fb09.uni-giessen.de/gbh/", "helene.garre", "") as api:
    print(api)
    datasets = api.dataset.list(site=3189) #central transect in Großmutz
    datasets_LAI = api.dataset.list(valuetype=34)
