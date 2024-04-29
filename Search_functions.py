#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Searching by string or hashtags

def search_tweets(query_string, start_date=None, end_date=None):
    # Prepare the base query with necessary filters
    base_filter = [
        {"term": {"isretweet": False}}  # Filter to exclude retweets
    ]

    # Check if start_date and end_date are provided and append date range filter
    if start_date and end_date:
        base_filter.append({
            "range": {
                "created_at": {
                    "gte": start_date,  # Greater than or equal to start_date
                    "lte": end_date,    # Less than or equal to end_date
                    "format": "yyyy-MM-dd"  # Ensure your dates are in this format or change it accordingly
                }
            }
        })

    # Define the query using the updated filter list
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
                        "filter": base_filter
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
    
    # Perform the search using the defined query
    results = es.search(index=index_name, body=query)  # Replace 'index_name' with the name of your index
    print("Search Results:\n" + "-"*80)
    for hit in results['hits']['hits']:
        tweet_data = hit['_source']
        score = hit['_score'] 
        print(f"Tweet ID: {tweet_data.get('id_str')}")
        print(f"Username: {tweet_data['user']['name']}")
        print(f"Created At: {tweet_data['created_at']}")
        print(f"Text: {tweet_data['text']}")
        print(f"Retweets: {tweet_data.get('retweet_count', 0)}")
        print(f"Likes: {tweet_data.get('favorite_count', 0)}")
        print(f"Engagement Score2: {tweet_data.get('engagement_score2', 'Not available')}")
        print(f"Combined Relevance Score: {score}")
        print("-"*80)

# Example usage
import time
start_time = time.time()
search_tweets("Corona", "2020-01-01", "2022-12-31")
end_time = time.time()
print(f"Total runtime of the program is {end_time - start_time} seconds")


# Searching all the retweets and tweets made by a User

def search_user_tweets(user_id):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"user.id_str": user_id}}  # Filter to match the user ID
                ]
            }
        },
        "sort": [
            {"created_at": {"order": "asc"}}  # Sort the tweets by creation date in ascending order
        ],
        "_source": [
            "created_at", "user.name", "text", "isretweet", "retweet_count", "favorite_count", "retweeted_status"
        ]  # Include retweet and like counts, and retweeted_status for extracting counts if it is a retweet
    }
    
    # Execute the search query
    results = es.search(index=index_name, body=query)#, size=1000)  # Adjust size as needed
    print("Search Results:\n" + "-"*80)  # Print a header and a separator
    for hit in results['hits']['hits']:
        # Extracting the specified fields from each tweet
        tweet_data = hit['_source']
        print(f"Tweet ID: {hit['_id']}")  # Using the document ID as Tweet ID
        print(f"Username: {tweet_data['user']['name']}")
        print(f"Created At: {tweet_data['created_at']}")
        print(f"Text: {tweet_data['text']}")

        # Check if the tweet is a retweet and extract like and retweet counts accordingly
        if tweet_data.get('isretweet', False):
            retweet_data = tweet_data.get('retweeted_status', {})
            retweet_count = retweet_data.get('retweet_count', 0)
            favorite_count = retweet_data.get('favorite_count', 0)
        else:
            retweet_count = tweet_data.get('retweet_count', 0)
            favorite_count = tweet_data.get('favorite_count', 0)

        print(f"Retweets: {retweet_count}")
        print(f"Likes: {favorite_count}")
        print(f"Is Retweet: {'Yes' if tweet_data.get('isretweet', False) else 'No'}")
        print("-"*80)  # Print a line after each tweet's details

# Example usage
start_time = time.time()
user_id = "1013800044695179265"
search_user_tweets("1013800044695179265")
end_time = time.time()
print(f"Total runtime of the program is {end_time - start_time} seconds")


# Searching a user based on string

#add a relavance order 

import pymysql.cursors
from elasticsearch import Elasticsearch

def search_user_string(search_string):

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

    # Connect to MySQL using pymysql
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='bhavana',
                                 database='USERS_DATA',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # MySQL query to fetch user details using a dynamic query for variable length user_ids
            format_strings = ','.join('%s' for _ in user_ids)
            query = f"""
            SELECT id, name, screen_name, location, verified, followers_count, statuses_count, friends_count
            FROM Users4
            WHERE id IN ({format_strings})
            ORDER BY FIELD(id, {','.join(user_ids)}), followers_count DESC
            """
            cursor.execute(query, tuple(user_ids))

            # Print user details
            results = cursor.fetchall()
            if results:
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
            else:
                print("No user found with the given IDs in MySQL.")
    finally:
        connection.close()

#Example usage
search_user_string(search_string="gol")


# Searching a user based on user id

import pymysql.cursors

def search_user_by_id(user_id):
    # Establish a database connection using pymysql
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='bhavana',
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
    
            # Fetch and print all the results
            results = cursor.fetchall()
            if results:
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
            else:
                print("No user found with the given user ID.")
    finally:
        connection.close()

# Example usage
search_user_by_id('841961917807767553')


# Searching all the Users who retweeted a particular Tweet

import pymysql.cursors
from elasticsearch import Elasticsearch

def find_retweeters_and_print_details(tweet_id):
    tweet_id=get_original_tweet_id(tweet_id)
    user_ids = set()

    # Fetch the original tweet details
    original_query = {
        "query": {
            "term": {"id_str": tweet_id}
        },
        "_source": ["id_str", "text", "user.id_str","favorite_count","retweet_count"],
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
            "term": {"retweeted_status.id_str": tweet_id}
        },
        "_source": ["id_str", "text", "user.id_str","favorite_count","retweet_count"],
        "size": 1000
    }
    retweet_response = es.search(index=index_name, body=retweet_query)

    if retweet_response['hits']['hits']:
        for hit in retweet_response['hits']['hits']:
            user_id = hit['_source']['user']['id_str']
            user_ids.add(user_id)

    # Ensure there are user IDs to query in MySQL
    if not user_ids:
        print("No user details to fetch.")
        return

    # Connect to MySQL using pymysql
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='bhavana',
                                 database='USERS_DATA',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # MySQL query to fetch user details
            format_strings = ','.join(['%s'] * len(user_ids))  # Ensures the correct number of placeholders
            query = f"""
            SELECT id, name, screen_name, location, verified, followers_count, statuses_count, friends_count
            FROM Users4
            WHERE id IN ({format_strings})
            """
            cursor.execute(query, tuple(user_ids))
            results = cursor.fetchall()

            # Dictionary to store user info for easy access
            users_info = {result['id']: result for result in results}

            # Print details of the original tweet's user
            if original_tweet_user_id in users_info:
                print("Original Tweet User Info:\n" + "="*60)
                user = users_info[original_tweet_user_id]
                print_user_info(user)
                print("="*60)

            # Print original tweet details
            if original_tweet:
                print(f"Original Tweet ID: {original_tweet['_source']['id_str']}")
                print(f"Original Tweet Text: {original_tweet['_source']['text']}")
                print(f"Likes: {original_tweet['_source']['favorite_count']}")
                print(f"Retweets: {original_tweet['_source']['retweet_count']}")
                print("-" * 60)

            # Print retweet details
            if retweet_response['hits']['hits']:
                print(f"Retweets of Tweet ID {tweet_id}:\n" + "-"*60)
                for hit in retweet_response['hits']['hits']:
                    retweet_id = hit['_source']['id_str']
                    retweet_text = hit['_source']['text']
                    retweeted_user_id = hit['_source']['user']['id_str']
                    print(f"Retweet ID: {retweet_id}\nRetweet Text: {retweet_text}\nRetweeted by User ID: {retweeted_user_id}")
                    if retweeted_user_id in users_info:
                        print_user_info(users_info[retweeted_user_id])
                    print("-" * 60)
            else:
                print("No retweets found for this tweet ID.")
    finally:
        connection.close()

def print_user_info(user):
    if user:
        print(f"User ID: {user['id']}\nName: {user['name']}\nScreen Name: {user['screen_name']}")
        print(f"Location: {user['location']}\nVerified: {'Yes' if user['verified'] else 'No'}")
        print(f"Followers Count: {user['followers_count']}\nStatuses Count: {user['statuses_count']}\nFriends Count: {user['friends_count']}")

# Example usage

def get_original_tweet_id(tweet_id):
    
    # Query to fetch the tweet by ID
    query = {
        "query": {
            "term": {"id_str": tweet_id}
        },
        "_source": ["id_str", "isretweet", "retweeted_status.id_str"]
    }
    
    # Execute the search
    response = es.search(index=index_name, body=query)  # Replace "tweets_index" with your index name
    
    # Check if any tweet was found
    if response['hits']['hits']:
        tweet = response['hits']['hits'][0]['_source']
        
        # Check if the tweet is a retweet
        if tweet.get('isretweet', False):
            # Return the original tweet ID from the retweeted_status if available
            return tweet.get('retweeted_status', {}).get('id_str', tweet_id)
        else:
            # Return the provided tweet ID as it's not a retweet
            return tweet_id
    else:
        # No tweet found by the given ID
        return None
    
find_retweeters_and_print_details("1249403835468009472")





# Top 10 Tweets (Computing each score at one at a time)

from elasticsearch import Elasticsearch
import pymysql.cursors



# Establish connection to MySQL using pymysql
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='bhavana',
                             database='USERS_DATA',
                             cursorclass=pymysql.cursors.DictCursor)

def fetch_user_details(user_id):
    with connection.cursor() as cursor:
        query = "SELECT name, screen_name, location, followers_count FROM users4 WHERE id = %s"
        cursor.execute(query, (user_id,))
        user_info = cursor.fetchone()
    return user_info

def custom_sort_tweets():
    query = {
        "size": 10,
        "query": {
            "bool": {
                "must": [
                    {"match": {"isretweet": False}}
                ],
                "must_not": [
                    {"exists": {"field": "retweeted_status"}}
                ]
            }
        },
        "sort": {
            "_script": {
                "type": "number",
                "script": {
                    "lang": "painless",
                    "source": "Math.pow(doc['retweet_count'].value, 2) + doc['favorite_count'].value"
                },
                "order": "desc"
            }
        }
    }
    results = es.search(index=index_name, body=query)
    print("Search Results:\n" + "-"*40)  # Print a header and a separator
    for result in results['hits']['hits']:
        tweet = result['_source']
        user_info = fetch_user_details(tweet['user']['id_str'])
        print(f"Tweet ID: {result['_id']}")
        print(f"Text: {tweet.get('text', 'No text available')}")
        print(f"Retweets: {tweet.get('retweet_count', 0)}")
        print(f"Likes: {tweet.get('favorite_count', 0)}")
        print("User Info:")
        if user_info:
            print(f"Name: {user_info['name']}")
            print(f"Screen Name: {user_info['screen_name']}")
            print(f"Location: {user_info['location']}")
            print(f"Followers: {user_info['followers_count']}")
        else:
            print("User details not found.")
        print("-" * 40)

# Run the function

start_time = time.time()
custom_sort_tweets()
end_time = time.time()
print(f"Total runtime of the program is {end_time - start_time} seconds")
# Close the database connection
connection.close()


# Top 10 Tweets (Ordering using the predefined metric)

from elasticsearch import Elasticsearch
import pymysql.cursors

# Establish connection to MySQL using pymysql
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='bhavana',
                             database='USERS_DATA',
                             cursorclass=pymysql.cursors.DictCursor)


def fetch_user_details(user_id):
    with connection.cursor() as cursor:
        query = "SELECT name, screen_name, location, followers_count FROM users4 WHERE id = %s"
        cursor.execute(query, (user_id,))
        user_info = cursor.fetchone()
    return user_info

def Top_10_tweets():
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
            {"engagement_score2": {"order": "desc"}}  # Sorting by engagement_score2
        ]
    }
    results = es.search(index=index_name, body=query)
    print("Search Results:\n" + "-"*40)  # Print a header and a separator
    for result in results['hits']['hits']:
        tweet = result['_source']
        user_info = fetch_user_details(tweet['user']['id_str'])
        print(f"Tweet ID: {result['_id']}")
        print(f"Text: {tweet.get('text', 'No text available')}")
        print(f"Retweets: {tweet.get('retweet_count', 0)}")
        print(f"Likes: {tweet.get('favorite_count', 0)}")
        print(f"Engagement Score2: {tweet.get('engagement_score2', 'Not available')}")  # Display the engagement_score2
        print("User Info:")
        if user_info:
            print(f"Name: {user_info['name']}")
            print(f"Screen Name: {user_info['screen_name']}")
            print(f"Location: {user_info['location']}")
            print(f"Followers: {user_info['followers_count']}")
        else:
            print("User details not found.")
        print("-" * 40)

start_time = time.time()
Top_10_tweets()
end_time = time.time()
print(f"Total runtime of the program is {end_time - start_time} seconds")

# Close the database connection
connection.close()


# Top 10 Users

import pymysql
import time

# Establishing connections
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='bhavana',
                             database='USERS_DATA',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

def get_top_10_users_by_followers():
    """ Fetch top 10 users sorted by followers count from MySQL """
    with connection.cursor() as cursor:
        # Query to fetch users sorted by followers count in descending order
        cursor.execute("SELECT id, name, screen_name, followers_count FROM users4 ORDER BY followers_count DESC LIMIT 10")
        top_users = cursor.fetchall()  # Fetches the top 10 users
        return top_users

def print_user_details(top_users):
    """ Print details of top users """
    print("Top 10 Users by Followers Count:\n" + "-" * 40)
    for user in top_users:
        print(f"User ID: {user['id']}")
        print(f"Name: {user['name']}")
        print(f"Screen Name: {user['screen_name']}")
        print(f"Followers Count: {user['followers_count']}")
        print("-" * 40)

# Main execution flow
start_time = time.time()
top_users = get_top_10_users_by_followers()
print_user_details(top_users)
end_time = time.time()
print(f"Total runtime of the program is {end_time - start_time} seconds")

# Closing the database connection
connection.close()


#approach,

# 1st take all the uniques user ids
# for each user id find out all the original tweets
# aggregate all the engagement_score2 for all the original tweets made by the user id
# now add followers count for userid to this agg engament_score2
# do it for all user and sort based on above

# print all the user info from mysql for top 10 users



# Establishing connections
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='bhavana',
                             database='USERS_DATA',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


def get_unique_user_ids():
    """ Fetch unique user IDs from the MySQL database """
    with connection.cursor() as cursor:
        cursor.execute("SELECT id FROM users4")
        user_ids = [row['id'] for row in cursor.fetchall()]
        return user_ids

def aggregate_engagement(user_id):
    """ For a given user ID, find all original tweets and sum up engagement_score2 """
    query = {
        
        "query": {
            "bool": {
                "must": [
                    {"term": {"user.id_str": user_id}},
                    {"term": {"isretweet": False}}
                ]
            }
        },
        "aggs": {
            "total_engagement": {
                "sum": {
                    "field": "engagement_score2"
                }
            }
        }
    }
    result = es.search(index=index_name, body=query)
    return result['aggregations']['total_engagement']['value']

def fetch_and_sort_users(user_ids):
    """ Fetch followers count from MySQL and calculate total score """
    user_scores = []
    with connection.cursor() as cursor:
        for user_id in user_ids:
            engagement_score = aggregate_engagement(user_id)
            cursor.execute("SELECT id, name, screen_name, followers_count FROM users4 WHERE id = %s", (user_id,))
            user = cursor.fetchone()
            if user:
              #  print(engagement_score)
                total_score = engagement_score + user['followers_count']
                user['total_score'] = total_score
                user_scores.append(user)
    # Sorting users by total score
    return sorted(user_scores, key=lambda x: x['total_score'], reverse=True)[:10]

def Top_10_users():
    user_ids = get_unique_user_ids()
    if user_ids:
        top_users = fetch_and_sort_users(user_ids)
        for user in top_users:
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
# Closing the database connection
connection.close()

