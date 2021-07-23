#!/usr/bin/env python
# coding: utf-8

# # Rate my professor scraper
# 
# Scraping data from https://www.ratemyprofessors.com/
#     
# 2 outputs
# 1. Teacher profile including teacher name, university, average score, average difficulty rating, and top tags
# 2. For each teacher, all their reviews

# In[1]:


# Data manipulation libraries
from tqdm.notebook import tqdm
import pandas as pd
import numpy as np
# Common webscraping libaries
from bs4 import BeautifulSoup as bs
import requests

tqdm.pandas()


# In[2]:


from time import sleep
import json

def get_teacher_data(school_id="U2Nob29sLTE0NjY=",increment=200):
    query = {'query': 'query TeacherSearchPaginationQuery(\n  $count: Int!\n  $cursor: String\n  $query: TeacherSearchQuery!\n) {\n  search: newSearch {\n    ...TeacherSearchPagination_search_1jWD3d\n  }\n}\n\nfragment TeacherSearchPagination_search_1jWD3d on newSearch {\n  teachers(query: $query, first: $count, after: $cursor) {\n    edges {\n      cursor\n      node {\n        ...TeacherCard_teacher\n        id\n        __typename\n      }\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n    }\n    resultCount\n  }\n}\n\nfragment TeacherCard_teacher on Teacher {\n  id\n  legacyId\n  avgRating\n  numRatings\n  ...CardFeedback_teacher\n  ...CardSchool_teacher\n  ...CardName_teacher\n  ...TeacherBookmark_teacher\n}\n\nfragment CardFeedback_teacher on Teacher {\n  wouldTakeAgainPercent\n  avgDifficulty\n}\n\nfragment CardSchool_teacher on Teacher {\n  department\n  school {\n    name\n    id\n  }\n}\n\nfragment CardName_teacher on Teacher {\n  firstName\n  lastName\n}\n\nfragment TeacherBookmark_teacher on Teacher {\n  id\n  isSaved\n}\n',
     'variables': {'count': increment,
      'query': {'text': '', 'schoolID': school_id}}}
    has_next_page = True
    teacher_data = []
    cursor = None
    headers = {
        'Host': 'www.ratemyprofessors.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Accept': '*/*',
        'Accept-Language': 'en-CA,en-US;q=0.7,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/json',
        'Authorization':'Basic dGVzdDp0ZXN0',
        'Origin': 'https://www.ratemyprofessors.com',
        'Content-Length': '1161',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Cookie': '_scid=786e8c25-e183-4670-a4bf-14c4e2f886f7; __browsiUID=5bc15d35-c2d0-44a9-854f-d74d758417c4; previousSchoolID=12502; promotionIndex=0; ad_blocker_overlay_2019=true; ccpa-notice-viewed-02=true; __browsiSessionID=8b668abf-46aa-465a-96d2-0f624e32a868&true&false&DEFAULT&gb&desktop-3.8.1&false'
    }
    url = 'https://www.ratemyprofessors.com/graphql'
    while has_next_page:
        sleep(1)
        r = requests.post(url,json=query,headers = headers)
        json_data = json.loads(r.text)['data']['search']['teachers']
        if not r.status_code == 200:
            print(r)
            break
        if 'edges' in json_data:
            teacher_page = json_data['edges']
            teacher_data += teacher_page
        # update cursor to start at next page
        if len(teacher_page): cursor = teacher_page[-1]['cursor']
        if query['variables'].get('cursor') == cursor:
            break
        query['variables']['cursor'] = cursor
#         print(cursor,has_next_page,len(teacher_data))
    return [x['node'] for x in teacher_data if 'node' in x]


# In[3]:


school_df = pd.read_csv("data/input/Schools CSV.csv")
# Drop schools that don't have an id (-1) or haven't been collected yet (n/a)
school_df = school_df[school_df["Ratemyprofessor ID"] != '-1'].dropna(subset=["Ratemyprofessor ID"])
# Scrape each page
teacher_data = school_df.progress_apply(lambda x : get_teacher_data(x['Ratemyprofessor ID']),
                                       axis=1).tolist()
# Flatten the list of lists into a single list
flattened = [element for list_ in teacher_data for element in list_]
teacher_df = pd.DataFrame.from_records(flattened)
teacher_df[['schoolId','schoolName']] = pd.json_normalize(teacher_df['school'])
teacher_df.drop(['__typename','isSaved','school'],axis=1,inplace=True)
teacher_df


# In[6]:


teacher_df['numRatings'].median()


# In[7]:


teacher_df = teacher_df[["firstName",
                         "lastName",
                         "department",
                         "avgRating",
                         "avgDifficulty",
                         "wouldTakeAgainPercent",
                         'numRatings',
                         "id",
                         "legacyId",
                         "schoolName",
                         "schoolId"]]
teacher_df.to_csv(f"data/output/teachers.csv",index=False)


# In[ ]:




