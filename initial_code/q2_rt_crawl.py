import bs4
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
import re
import pandas as pd
import numpy as np
year_to_crawl = 2020
my_url = f'https://www.rottentomatoes.com/top/bestofrt?year={str(year_to_crawl)}'

# grabbing connection
uClient = uReq(my_url)
page_html = uClient.read()
uClient.close()

# html parser
page_soup = soup(page_html, "html.parser")

# gather movies
containers = page_soup.findAll("table", {"class":"table"})

fact_top_movies = pd.DataFrame()

for container in containers:
    temp_df = pd.DataFrame(data={'year':[year_to_crawl]})
    movie_rank_container = container.findAll("td", {"class":"bold"})
    movie_title_container = container.findAll("a", {"class":"unstyled articleLink"})
    movie_review_container = container.findAll("td", {"class":"right hidden-xs"})

    for movie_rank, movie_titles, movie_review in zip(movie_rank_container, movie_title_container, movie_review_container):
        rank = movie_rank.text.strip('.')
        temp_df['rank'] = rank

        title = movie_titles.text.strip()
        temp_df['title'] = title

        url = movie_titles['href'].strip()
        temp_df['url'] = url

        review = movie_review.text.strip()
        temp_df['reviews'] = review

        print("rank: " + rank)
        print("title: " + title)
        print("url: " + url)
        print("review: " + review)

        my_url = 'https://www.rottentomatoes.com' + url
        print(my_url)
        # grabbing connection
        uClient = uReq(my_url)
        page_html = uClient.read()
        uClient.close()

        # html parser
        page_soup = soup(page_html, "html.parser")

        score_container = page_soup.find("score-board",attrs={'class': 'scoreboard'})
        audience_score = score_container['audiencescore'] 
        temp_df['audience_score'] = audience_score

        tomatometer = score_container['tomatometerscore']
        temp_df['tomatometer'] = tomatometer

        synopsis_container = page_soup.find('div', {"class": "movie_synopsis"})
        synopsis = synopsis_container.text.strip()        
        temp_df['synopsis'] = synopsis

        movie_label_container = page_soup.findAll('div', attrs={"data-qa":"movie-info-item-label"})
        movie_value_container = page_soup.findAll('div', attrs={"data-qa":"movie-info-item-value"})
        
        for movie_label, movie_value in zip(movie_label_container, movie_value_container):
            # print(movie_label.text.strip(), movie_value.text.strip())
            info_label = movie_label.text.strip().replace(':','').lower()
            info_value = movie_value.text.strip().replace(':','')

            print(info_label,info_value)
            temp_df[info_label] = info_value
        
        fact_top_movies = pd.concat([fact_top_movies,temp_df])

    fact_top_movies['producer'] = fact_top_movies['producer'].str.replace('  ','')
    fact_top_movies['director'] = fact_top_movies['director'].str.replace('  ','')
    fact_top_movies['writer'] = fact_top_movies['writer'].str.replace('  ','')
    

    fact_top_movies['split_runtime_hours'] = fact_top_movies['runtime'].str.split(' ').str[0].str.split('h').str[0]
    fact_top_movies['split_runtime_hours'] =  np.where(fact_top_movies['split_runtime_hours'].str.contains('m'), pd.to_numeric(fact_top_movies['split_runtime_hours'].str.split('m').str[0]) /60, fact_top_movies['split_runtime_hours'])
    fact_top_movies['split_runtime_hours'] = fact_top_movies['split_runtime_hours'].astype('float')

    fact_top_movies['split_runtime_mins'] = fact_top_movies['runtime'].str.split(' ').str[1].str.split('m').str[0]
    fact_top_movies['split_runtime_mins'] = np.where(fact_top_movies['split_runtime_mins'].isnull(), 0, fact_top_movies['split_runtime_mins'])
    fact_top_movies['split_runtime_mins'] = fact_top_movies['split_runtime_mins'].astype('float')
    
    fact_top_movies['runtime_mins'] = (fact_top_movies['split_runtime_hours'] * 60) +  fact_top_movies['split_runtime_mins']
    fact_top_movies['runtime_mins'] = fact_top_movies['runtime_mins'].astype('int')

    fact_top_movies.rename(columns={'original language': 'original_language', 
                                    'release date (theaters)': 'release_date_theaters',
                                    'release date (streaming)': 'release_date_streaming',
                                    'aspect ratio': 'aspect_ratio',
                                    'view the collection': 'production_co'},
                                     inplace=True)

    fact_top_movies['year'] = fact_top_movies['year'].astype('int')
    fact_top_movies['reviews'] = fact_top_movies['reviews'].astype('int')
    fact_top_movies['tomatometer'] = fact_top_movies['tomatometer'].astype('float')

    fact_top_movies['audience_score'] =fact_top_movies['audience_score']
    fact_top_movies['audience_score'] = np.where(fact_top_movies['audience_score']=='','0',fact_top_movies['audience_score'])

    fact_top_movies['audience_score'] = fact_top_movies['audience_score'].astype('float')
    fact_top_movies[fact_top_movies['audience_score']=='']

    fact_top_movies['release_date_theaters'] = fact_top_movies['release_date_theaters'].str.split('\n').str[0]
    fact_top_movies['release_date_theaters'] = pd.to_datetime(fact_top_movies['release_date_theaters'])
    
    fact_top_movies['release_date_streaming'] = fact_top_movies['release_date_streaming'].str.split('\n').str[0]
    fact_top_movies['release_date_streaming'] = pd.to_datetime(fact_top_movies['release_date_streaming'])
    
    fact_top_movies.drop(columns=['rank'],errors='ignore',axis=1,inplace=True)
    fact_top_movies.drop(columns=['box office (gross usa)'],errors='ignore',axis=1,inplace=True)
    fact_top_movies.drop(columns=['sound mix'],errors='ignore',axis=1,inplace=True)
    fact_top_movies.drop(columns=['split_runtime_hours'],errors='ignore',axis=1,inplace=True)
    fact_top_movies.drop(columns=['split_runtime_mins'],errors='ignore',axis=1,inplace=True)
    fact_top_movies.drop(columns=['runtime'],errors='ignore',axis=1,inplace=True)
    
    fact_top_movies.info()
    fact_top_movies.to_csv('check.csv',index=False)