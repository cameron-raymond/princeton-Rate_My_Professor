#!/usr/bin/env python
# coding: utf-8

# # Rate my professor review scraper
# 
# Scraping data from https://www.ratemyprofessors.com/
#     
# Output: For each teacher, all their reviews


# Data manipulation libraries
from tqdm import tqdm
from glob import glob
import pandas as pd
import numpy as np
# Common webscraping libaries
from bs4 import BeautifulSoup as bs
import requests

tqdm.pandas()

print("Initializing Scraper")


from time import sleep
import json

def get_review_data(teacher_id,increment=200):
	query = {"query":"query RatingsListQuery(\n  $count: Int!\n  $id: ID!\n  $courseFilter: String\n  $cursor: String\n) {\n  node(id: $id) {\n    __typename\n    ... on Teacher {\n      ...RatingsList_teacher_4pguUW\n    }\n    id\n  }\n}\n\nfragment RatingsList_teacher_4pguUW on Teacher {\n  id\n  lastName\n  numRatings\n  school {\n    id\n    legacyId\n    name\n    city\n    state\n    avgRating\n    numRatings\n  }\n  ...Rating_teacher\n  ...NoRatingsArea_teacher\n  ratings(first: $count, after: $cursor, courseFilter: $courseFilter) {\n    edges {\n      cursor\n      node {\n        ...Rating_rating\n        id\n        __typename\n      }\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n    }\n  }\n}\n\nfragment Rating_teacher on Teacher {\n  ...RatingFooter_teacher\n  ...RatingSuperHeader_teacher\n  ...ProfessorNoteSection_teacher\n}\n\nfragment NoRatingsArea_teacher on Teacher {\n  lastName\n  ...RateTeacherLink_teacher\n}\n\nfragment Rating_rating on Rating {\n  comment\n  teacherNote {\n    id\n  }\n  ...RatingHeader_rating\n  ...RatingSuperHeader_rating\n  ...RatingValues_rating\n  ...CourseMeta_rating\n  ...RatingTags_rating\n  ...RatingFooter_rating\n  ...ProfessorNoteSection_rating\n}\n\nfragment RatingHeader_rating on Rating {\n  date\n  class\n  helpfulRating\n  clarityRating\n  isForOnlineClass\n}\n\nfragment RatingSuperHeader_rating on Rating {\n  legacyId\n}\n\nfragment RatingValues_rating on Rating {\n  helpfulRating\n  clarityRating\n  difficultyRating\n}\n\nfragment CourseMeta_rating on Rating {\n  attendanceMandatory\n  wouldTakeAgain\n  grade\n  textbookUse\n  isForOnlineClass\n  isForCredit\n}\n\nfragment RatingTags_rating on Rating {\n  ratingTags\n}\n\nfragment RatingFooter_rating on Rating {\n  id\n  comment\n  adminReviewedAt\n  flagStatus\n  legacyId\n  thumbsUpTotal\n  thumbsDownTotal\n  thumbs {\n    userId\n    thumbsUp\n    thumbsDown\n    id\n  }\n  teacherNote {\n    id\n  }\n}\n\nfragment ProfessorNoteSection_rating on Rating {\n  teacherNote {\n    ...ProfessorNote_note\n    id\n  }\n  ...ProfessorNoteEditor_rating\n}\n\nfragment ProfessorNote_note on TeacherNotes {\n  comment\n  ...ProfessorNoteHeader_note\n  ...ProfessorNoteFooter_note\n}\n\nfragment ProfessorNoteEditor_rating on Rating {\n  id\n  legacyId\n  class\n  teacherNote {\n    id\n    teacherId\n    comment\n  }\n}\n\nfragment ProfessorNoteHeader_note on TeacherNotes {\n  createdAt\n  updatedAt\n}\n\nfragment ProfessorNoteFooter_note on TeacherNotes {\n  legacyId\n  flagStatus\n}\n\nfragment RateTeacherLink_teacher on Teacher {\n  legacyId\n  numRatings\n  lockStatus\n}\n\nfragment RatingFooter_teacher on Teacher {\n  id\n  legacyId\n  lockStatus\n  isProfCurrentUser\n}\n\nfragment RatingSuperHeader_teacher on Teacher {\n  firstName\n  lastName\n  legacyId\n  school {\n    name\n    id\n  }\n}\n\nfragment ProfessorNoteSection_teacher on Teacher {\n  ...ProfessorNote_teacher\n  ...ProfessorNoteEditor_teacher\n}\n\nfragment ProfessorNote_teacher on Teacher {\n  ...ProfessorNoteHeader_teacher\n  ...ProfessorNoteFooter_teacher\n}\n\nfragment ProfessorNoteEditor_teacher on Teacher {\n  id\n}\n\nfragment ProfessorNoteHeader_teacher on Teacher {\n  lastName\n}\n\nfragment ProfessorNoteFooter_teacher on Teacher {\n  legacyId\n  isProfCurrentUser\n}\n",
             "variables": { "count": increment,
                           "id": teacher_id}}
	has_next_page = True
	review_data = []
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
		if not r.status_code == 200:
			print(r)
			break
		json_data = json.loads(r.text)
		if "errors" in json_data:
			es = [e['message'] for e in json_data['errors']]
			print(" & ".join(es))
			break
		json_data = json_data['data']['node']['ratings']
		if 'edges' in json_data:
			teacher_page = json_data['edges']
			review_data += teacher_page
        	# update cursor to start at next page
		if len(teacher_page): cursor = teacher_page[-1]['cursor']
		if query['variables'].get('cursor') == cursor:
			break
		query['variables']['cursor'] = cursor
#         	print(cursor,has_next_page,len(review_data))
	return [{**x['node'], 'teacherId': teacher_id} for x in review_data if 'node' in x]


if __name__ == "__main__":
	teacher_overview_df = pd.read_csv("data/output/teachers.csv")
	teacher_overview_df = teacher_overview_df[teacher_overview_df['numRatings'] > 0]
	chunk_size = 100
	num_segments = len(teacher_overview_df)//chunk_size
	print(f"{len(teacher_overview_df)} teachers split up into {num_segments} chunks of size {chunk_size}")
	existing_chunks = sorted(glob("data/output/reviews/*"))
	last_chunk = int(existing_chunks[-1].split("/")[-1].split("-")[0])
	chunks = np.array_split(teacher_overview_df,num_segments)[last_chunk+1:]
	print(f"{last_chunk} chunks already collected, {len(chunks)} remaining")
	i=last_chunk+1
	for chunk in tqdm(chunks):	
		print(f"Chunk {i}")
        	# Scrape each page
		review_data = chunk.apply(lambda x : get_review_data(x['id']),
                                    axis=1).tolist()
        	# Flatten the list of lists into a single list
		flattened = [element for list_ in review_data for element in list_]
		review_df = pd.DataFrame.from_records(flattened)
		review_df = review_df.merge(teacher_overview_df[["id", "firstName", "lastName","department","schoolName"]],
                                how="left",
                                left_on="teacherId",
                                right_on="id")
		review_df.drop(['__typename','adminReviewedAt','id_y','flagStatus'],axis=1,inplace=True)
		review_df.rename({'id_x': 'reviewId'},axis=1,inplace=True)
		review_df = review_df[["firstName",
                        "lastName",
                        'teacherId',
                        "department",
                        "schoolName",
                        'class',
                        'date',
                        'reviewId',
                        'clarityRating',
                        'difficultyRating',
                        'helpfulRating',
                        'wouldTakeAgain',
                        'textbookUse',
                        'comment',
                        'ratingTags',
                        'teacherNote',
                        'grade',
                        'attendanceMandatory',
                        'isForCredit', 
                        'isForOnlineClass',
                        'thumbs', 
                        'thumbsDownTotal', 
                        'thumbsUpTotal']]
		if not i%100:
			print(f"{i}! Saving CSV for safety")
			review_df.to_csv(f"data/output/{i:06d}-reviews.csv",index=False)
		review_df.to_pickle(f"data/output/reviews/{i:06d}-reviews.pkl")
		i +=1
	print("Finished scraping")
