# Twitter Search Application – Team 28

## Abstract
The **Twitter Search Application** is a service designed to automatically submit searches to Twitter and retrieve matching tweets. It is primarily used to gather tweets for research purposes, such as social science studies. This project introduces a method to check the coverage of its results for narrowly focused topics and demonstrates efficient data handling, caching, and execution time comparison with and without caching.

---

## Introduction
Twitter generates over 500 million messages daily, covering diverse topics like music, sports, politics, and technology. This project aims to develop a system that searches for tweets efficiently by users, tweets, or hashtags. It also implements a caching mechanism to compare execution times with and without caching.

---

## Dataset
The **Corona-2** and **Corona-3** datasets were used for this application. These datasets were loaded into:
- **MySQL Server** for relational data storage.
- **Elasticsearch** for non-relational data storage.

Necessary fields were extracted from JSON data and stored in the respective databases.

---

## Technologies Used
### MySQL Server
Stores **Users Data** with the following fields:
- `name`, `screen_name`, `user_id_str` (primary key)
- `follower_count`, `friend_count`, `status_count`
- `location`, `verified`

### Elasticsearch
Chosen for its ability to handle large volumes of data with low latency. It is used to store **Tweets Data**, with a focus on efficient full-text search capabilities.

---

## Data Modeling
### Relational Datastore
- Defined schema: `users_data`.
- Skips duplicate `user_id` entries when loading JSON data into MySQL.

### Non-Relational Datastore
- Two mappings for Elasticsearch:
  1. **Mapping 1**: Optimized for searches by `user_id`, `name`, and `screen_name`.
  2. **Mapping 2**: Stores tweets with selected fields and an additional `engagement_score` field for ranking search results.

---

## Architecture

<img width="352" alt="image" src="https://github.com/user-attachments/assets/db9195fe-f51e-474d-a7da-fb3f087b2864" />

---

## Preprocessing of Data
The dataset contains irrelevant and inconsistent data that needs cleaning. For instance:
- Missing original tweets for retweets were addressed by creating dummy entries.
- Engagement scores were calculated using:
engagement_score = (retweet)^2 + likes combined_score = √engagement_score + Score_elasticsearch

yaml
Copy code
This ensures accurate and relevant search results.

---

## Loading Approach
1. Parse JSON file line by line.
2. Check if the tweet exists in Elasticsearch.
3. Extract relevant fields from `User` and `Tweet` objects.
4. Insert tweets into Elasticsearch and user details into MySQL if not already present.
5. Handle retweets by linking them to original tweets.

---

## Caching
Implemented **LRU Cache** (Least Recently Used) using an ordered dictionary with a capacity of 7. 
### Features:
- **Durability**: Checkpoints saved every 60 minutes.
- **Functions**:
- `get(key)`: Retrieve value or return `None`.
- `put(key, value)`: Add or update key-value pairs, removing least recently used items if capacity is exceeded.
- `save_on_disk()` & `load_from_disk()`: Save/load cache to/from disk.
- **Cache Key Generation**:
- Example: `cache_key = f"user_tweets_{user_id}"`.

---

## Search Functionalities
1. **Search Users**: Based on `name` or `screen_name` using MySQL.
2. **Search Tweets**: Based on text or hashtags with optional time filters. Results are ranked by `combined_score`.
3. **Users Retweeting a Tweet**: Retrieve details of retweeters and the original tweeter.
4. **Top 10 Tweets**: Ranked by `engagement_score`.
5. **Top 10 Users**: Ranked by a combination of engagement scores and follower counts.

---

## Results
### Query Execution Times:
- User details: **0.0468 seconds**
- Tweets by string/hashtags: **0.0427 seconds**
- Users retweeting a tweet: **0.0524 seconds**
- Top 10 Tweets: **0.0435 seconds**
- Top 10 Users: **24.7081 seconds**

### Impact of Caching:
Caching improves data retrieval performance by approximately 100x, significantly reducing query execution time.

---

## Conclusion
The project demonstrates the impact of caching and efficient data modeling. A robust schema and mapping strategy reduce memory usage and improve performance. Caching enhances data retrieval speeds, highlighting its importance in large-scale applications.



