#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Top 10 tweets(Using Cache)

from elasticsearch import Elasticsearch
import pymysql.cursors
import time

# Establish connections
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='layakishore',
                             database='USERS_DATA',
                             cursorclass=pymysql.cursors.DictCursor)


def fetch_user_details(user_id):
    user_info = cache.get(f"user_{user_id}")
    if user_info:
        return user_info
    with connection.cursor() as cursor:
        query = "SELECT name, screen_name, location, followers_count FROM users4 WHERE id = %s"
        cursor.execute(query, (user_id,))
        user_info = cursor.fetchone()
        cache.put(f"user_{user_id}", user_info)
    return user_info

def Top_10_tweets():
    
    cached_tweets = cache.get('top_10_tweets')
    if cached_tweets:
        for tweet in cached_tweets:
            print_tweet_info(tweet)
        return

    query = {
        "size": 10,
        "query": {
            "bool": {
                "must": [
                    {"match": {"isretweet": False}}
                ],
            }
        },
        "sort": [
            {"engagement_score2": {"order": "desc"}}
        ]
    }
    results = es.search(index=index_name, body=query)
    tweets_info = []
    print("Search Results:\n" + "-"*40)
    for result in results['hits']['hits']:
        tweet = result['_source']
        user_info = fetch_user_details(tweet['user']['id_str'])
        tweet_info = {
            "tweet_id": result['_id'],
            "text": tweet.get('text', 'No text available'),
            "retweets": tweet.get('retweet_count', 0),
            "likes": tweet.get('favorite_count', 0),
            "engagement_score2": tweet.get('engagement_score2', 'Not available'),
            "user_info": user_info
        }
        tweets_info.append(tweet_info)
        print_tweet_info(tweet_info)

    cache.put('top_10_tweets', tweets_info)

def print_tweet_info(tweet):
    print(f"Tweet ID: {tweet['tweet_id']}")
    print(f"Text: {tweet['text']}")
    print(f"Retweets: {tweet['retweets']}")
    print(f"Likes: {tweet['likes']}")
    print(f"Engagement Score2: {tweet['engagement_score2']}")
    print("User Info:")
    if tweet['user_info']:
        user_info = tweet['user_info']
        print(f"Name: {user_info['name']}")
        print(f"Screen Name: {user_info['screen_name']}")
        print(f"Location: {user_info['location']}")
        print(f"Followers: {user_info['followers_count']}")
    print("-" * 40)

start_time = time.time()
Top_10_tweets()
end_time = time.time()
print(f"Total runtime of the program is {end_time - start_time} seconds")

# Close the database connection
connection.close()





# Top 10 Users(Using Cache)

connection = pymysql.connect(host='localhost',
                             user='root',
                             password='layakishore',
                             database='USERS_DATA',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

def Top_10_users():
    
    cache_key = "top_10_users"
    top_users = cache.get(cache_key)
    if top_users:
        print("Retrieved top users from cache.")
        for user in top_users:
            print_user_info(user)
        return
    
    user_ids = get_unique_user_ids()
    if user_ids:
        top_users = fetch_and_sort_users(user_ids)
        cache.put(cache_key, top_users)  # Cache the results
        
        for user in top_users:
            print_user_info(user)
    else:
        print("No user IDs available to process.")

def print_user_info(user):
    print("User ID:", user['id'])
    print("Name:", user['name'])
    print("Screen Name:", user['screen_name'])
    print("Followers Count:", user['followers_count'])
    print("Total Score:", user['total_score'])
    print("-" * 40)

start_time = time.time()
Top_10_users()
end_time = time.time()
print(f"Total runtime of the program is {end_time - start_time} seconds")
# Save cache state before closing the application



# Searching by string or hashtags(Using Cache)

from elasticsearch import Elasticsearch
import time



def search_tweets(query_string):
    cache_key = f"search_{query_string}"
    cached_results = cache.get(cache_key)
    
    if cached_results:
        print("Using cached results for:", query_string)
        print_search_results(cached_results)
        return
    
    query = {
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "must": {
                            "match": {
                                "text": query_string
                            }
                        },
                        "filter": {
                            "term": {
                                "isretweet": False
                            }
                        }
                    }
                },
                "functions": [
                    {
                        "field_value_factor": {
                            "field": "engagement_score2",
                            "factor": 1.2,
                            "modifier": "sqrt",
                            "missing": 1
                        }
                    }
                ],
                "boost_mode": "sum"
            }
        },
        "_source": ["created_at", "user.name", "text", "id_str", "retweet_count", "favorite_count", "engagement_score2"],
        "size": 10
    }
    
    results = es.search(index=index_name, body=query)
    formatted_results = format_search_results(results)
    cache.put(cache_key, formatted_results)  # Cache the new results
    print_search_results(formatted_results)

def format_search_results(results):
    return [{
        "id_str": hit['_source'].get('id_str'),
        "username": hit['_source']['user']['name'],
        "created_at": hit['_source']['created_at'],
        "text": hit['_source']['text'],
        "retweets": hit['_source'].get('retweet_count', 0),
        "likes": hit['_source'].get('favorite_count', 0),
        "engagement_score2": hit['_source'].get('engagement_score2', 'Not available'),
        "score": hit['_score']
    } for hit in results['hits']['hits']]

def print_search_results(results):
    print("Search Results:\n" + "-"*80)
    for result in results:
        print(f"Tweet ID: {result['id_str']}")
        print(f"Username: {result['username']}")
        print(f"Created At: {result['created_at']}")
        print(f"Text: {result['text']}")
        print(f"Retweets: {result['retweets']}")
        print(f"Likes: {result['likes']}")
        print(f"Engagement Score2: {result['engagement_score2']}")
        print(f"Combined Relevance Score: {result['score']}")
        print("-"*80)

# Example usage
start_time = time.time()
search_tweets("Football")
end_time = time.time()
print(f"Total runtime of the program is {end_time - start_time} seconds")


# Searching all the retweets and tweets made by a User(Using Cache)

def search_user_tweets(user_id):
    cache_key = f"user_tweets_{user_id}"
    cached_tweets = cache.get(cache_key)
    
    if cached_tweets:
        print("Using cached results for user ID:", user_id)
        print_search_results(cached_tweets)
        return

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"user.id_str": user_id}}
                ]
            }
        },
        "sort": [
            {"created_at": {"order": "asc"}}
        ],
        "_source": [
            "created_at", "user.name", "text", "isretweet", "retweet_count", "favorite_count", "retweeted_status"
        ],
        "size": 100  # Adjust based on your needs
    }
    
    results = es.search(index=index_name, body=query)
    formatted_results = format_tweet_results(results)
    cache.put(cache_key, formatted_results)  # Cache the results
    print_search_results(formatted_results)

def format_tweet_results(results):
    return [{
        "id": hit['_id'],
        "username": hit['_source']['user']['name'],
        "created_at": hit['_source']['created_at'],
        "text": hit['_source']['text'],
        "retweets": hit['_source'].get('retweet_count', 0),
        "likes": hit['_source'].get('favorite_count', 0),
        "is_retweet": hit['_source'].get('isretweet', False)
    } for hit in results['hits']['hits']]

def print_search_results(tweets):
    print("Search Results:\n" + "-"*80)
    for tweet in tweets:
        print(f"Tweet ID: {tweet['id']}")
        print(f"Username: {tweet['username']}")
        print(f"Created At: {tweet['created_at']}")
        print(f"Text: {tweet['text']}")
        print(f"Retweets: {tweet['retweets']}")
        print(f"Likes: {tweet['likes']}")
        print(f"Is Retweet: {'Yes' if tweet['is_retweet'] else 'No'}")
        print("-"*80)

# Example usage

start_time = time.time()
user_id = "1240111705599991809"
search_user_tweets(user_id)
end_time = time.time()
print(f"Total runtime of the program is {end_time - start_time} seconds")

# Searching a user based on string(Using Cache)

def search_user_string(search_string):
    cache_key = f"user_search_{search_string}"
    cached_results = cache.get(cache_key)
    
    if cached_results:
        print("Using cached results for search string:", search_string)
        print_user_details(cached_results)
        return
    
    user_ids = []

    if search_string:
        # Elasticsearch query to find relevant user_ids
        query_body = {
            "query": {
                "bool": {
                    "must_not": [
                        {"exists": {"field": "retweeted_status"}}
                    ],
                    "should": [
                        {"match_phrase_prefix": {"user.name": {"query": search_string, "max_expansions": 50}}},
                        {"wildcard": {"user.screen_name": f"*{search_string}*"}}
                    ],
                    "minimum_should_match": 1
                }
            },
            "_source": ["user.id_str"],
            "size": 1000  # Adjust size if necessary
        }
        response = es.search(index=index_name, body=query_body)
        user_ids = [hit['_source']['user']['id_str'] for hit in response['hits']['hits']]
    
    if not user_ids:
        print("No user found with the given information.")
        return

    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='layakishore',
                                 database='USERS_DATA',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            format_strings = ','.join('%s' for _ in user_ids)
            query = f"""
            SELECT id, name, screen_name, location, verified, followers_count, statuses_count, friends_count
            FROM Users4
            WHERE id IN ({format_strings})
            """
            cursor.execute(query, tuple(user_ids))
            results = cursor.fetchall()
            if results:
                cache.put(cache_key, results)  # Cache the results
                print_user_details(results)
            else:
                print("No user found with the given IDs in MySQL.")
    finally:
        connection.close()

def print_user_details(results):
    for result in results:
        print(f"ID: {result['id']}")
        print(f"Name: {result['name']}")
        print(f"Screen Name: {result['screen_name']}")
        print(f"Location: {result['location']}")
        print(f"Verified: {'Yes' if result['verified'] else 'No'}")
        print(f"Followers Count: {result['followers_count']}")
        print(f"Statuses Count: {result['statuses_count']}")
        print(f"Friends Count: {result['friends_count']}")
        print("-" * 40)

# Example usage

start_time = time.time()
search_user_string("gol")
end_time = time.time()
print(f"Total runtime of the program is {end_time - start_time} seconds")

# Searching a user based on user id(Using Cache)

import pymysql.cursors

# Assuming cache is an instance of LRUCache
def search_user_by_id(user_id):
    cache_key = f"user_details_{user_id}"
    cached_user = cache.get(cache_key)
    
    if cached_user:
        print("Using cached results for user ID:", user_id)
        print_user_details(cached_user)
        return
    
    # Establish a database connection using pymysql
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='layakishore',
        database='USERS_DATA',
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        with connection.cursor() as cursor:
            # Prepare the query
            query = """
            SELECT id, name, screen_name, location, verified, followers_count, statuses_count, friends_count
            FROM Users4
            WHERE id = %s
            """
    
            # Execute the query with the provided user ID
            cursor.execute(query, (user_id,))
    
            # Fetch the results
            results = cursor.fetchall()
            if results:
                cache.put(cache_key, results)  # Cache the results
                print_user_details(results)
            else:
                print("No user found with the given user ID.")
    finally:
        connection.close()

def print_user_details(results):
    for result in results:
        print(f"ID: {result['id']}")
        print(f"Name: {result['name']}")
        print(f"Screen Name: {result['screen_name']}")
        print(f"Location: {result['location']}")
        print(f"Verified: {'Yes' if result['verified'] else 'No'}")
        print(f"Followers Count: {result['followers_count']}")
        print(f"Statuses Count: {result['statuses_count']}")
        print(f"Friends Count: {result['friends_count']}")
        print("-" * 40)

# Example usage
start_time = time.time()
search_user_by_id('14135350')
end_time = time.time()
print(f"Total runtime of the program is {end_time - start_time} seconds")



# Searching all the Users who retweeted a particular Tweet(Using Cache)

import pymysql.cursors
from elasticsearch import Elasticsearch


def find_retweeters_and_print_details(tweet_id):
    cache_key = f"retweet_details_{tweet_id}"
    cached_data = cache.get(cache_key)
    
    if cached_data:
        print("Using cached results for tweet ID:", tweet_id)
        print_cached_tweet_details(cached_data)
        return
    
    original_tweet_id = get_original_tweet_id(tweet_id)
    user_ids = set()

    # Fetch the original tweet details
    original_query = {
        "query": {
            "term": {"id_str": original_tweet_id}
        },
        "_source": ["id_str", "text", "user.id_str", "favorite_count", "retweet_count"],
        "size": 1
    }
    original_response = es.search(index=index_name, body=original_query)

    original_tweet = None
    if original_response['hits']['hits']:
        original_tweet = original_response['hits']['hits'][0]
        original_tweet_user_id = original_tweet['_source']['user']['id_str']
        user_ids.add(original_tweet_user_id)

    # Fetch all retweets of the given tweet_id
    retweet_query = {
        "query": {
            "term": {"retweeted_status.id_str": original_tweet_id}
        },
        "_source": ["id_str", "text", "user.id_str", "favorite_count", "retweet_count"],
        "size": 1000
    }
    retweet_response = es.search(index=index_name, body=retweet_query)

    if retweet_response['hits']['hits']:
        for hit in retweet_response['hits']['hits']:
            user_id = hit['_source']['user']['id_str']
            user_ids.add(user_id)

    if not user_ids:
        print("No user details to fetch.")
        return

    # Fetch user details from MySQL
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='layakishore',
                                 database='USERS_DATA',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            format_strings = ','.join(['%s'] * len(user_ids))  # Ensures the correct number of placeholders
            query = f"SELECT id, name, screen_name, location, verified, followers_count, statuses_count, friends_count FROM Users4 WHERE id IN ({format_strings})"
            cursor.execute(query, tuple(user_ids))
            results = cursor.fetchall()
            user_details = {result['id']: result for result in results}
            cache_data = {
                "original_tweet": original_tweet['_source'] if original_tweet else None,
                "retweets": [hit['_source'] for hit in retweet_response['hits']['hits']],
                "user_details": user_details
            }
            cache.put(cache_key, cache_data)  # Cache the results
            print_cached_tweet_details(cache_data)
    finally:
        connection.close()

def print_cached_tweet_details(cached_data):
    original_tweet = cached_data['original_tweet']
    if original_tweet:
        print(f"Original Tweet ID: {original_tweet['id_str']}")
        print(f"Text: {original_tweet['text']}")
        print(f"Likes: {original_tweet['favorite_count']}")
        print(f"Retweets: {original_tweet['retweet_count']}")
        print("Original Tweet User Info:")
        original_user_id = original_tweet['user']['id_str']
        if original_user_id in cached_data['user_details']:
            print_user_info(cached_data['user_details'][original_user_id])
        print("-" * 80)
    
    print("Retweet Details:")
    for retweet in cached_data['retweets']:
        print(f"Retweet ID: {retweet['id_str']}")
        print(f"Text: {retweet['text']}")
        retweet_user_id = retweet['user']['id_str']
        if retweet_user_id in cached_data['user_details']:
            print_user_info(cached_data['user_details'][retweet_user_id])
        print("-" * 80)

def print_user_info(user):
    print(f"User ID: {user['id']}")
    print(f"Name: {user['name']}")
    print(f"Screen Name: {user['screen_name']}")
    print(f"Location: {user['location']}")
    print(f"Verified: {'Yes' if user['verified'] else 'No'}")
    print(f"Followers Count: {user['followers_count']}")
    print(f"Statuses Count: {user['statuses_count']}")
    print(f"Friends Count: {user['friends_count']}")
    print("-" * 40)




# Example usage
start_time = time.time()
find_retweeters_and_print_details("1249403835468009472")
end_time = time.time()
print(f"Total runtime of the program is {end_time - start_time} seconds")


