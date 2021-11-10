import os
import sqlite3
from TwitterAPI import TwitterAPI
import time

# create the database to store tweet information

con = sqlite3.connect("dbTweets.db")
cur = con.cursor()

def db():
    with con:
        cur.execute("""CREATE TABLE IF NOT EXISTS Tweet (tweet_id INTEGER, tweet_text TEXT, created_at TEXT, author_id INTEGER, context_annotations TEXT, entities TEXT, conversation_id INTEGER, retweet_count INTEGER, reply_count INTEGER, like_count INTEGER, quote_count INTEGER, reply_settings TEXT, source TEXT, withheld TEXT, attachments TEXT, UNIQUE(tweet_id))""")
        cur.execute("""CREATE INDEX IF NOT EXISTS Index_TweetID ON Tweet (tweet_id)""")
        cur.execute("""CREATE INDEX IF NOT EXISTS Index_AuthorID ON Tweet (author_id)""")


def insertTweet(tweet_id, tweet_text, created_at, author_id, context_annotations, entities, conversation_id, retweet_count,
                reply_count, like_count, quote_count, reply_settings, source, withheld, attachments):

    with con:
        cur.execute("INSERT OR IGNORE INTO Tweet VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (tweet_id, tweet_text, created_at, author_id, context_annotations, entities, conversation_id, retweet_count, reply_count, like_count, quote_count, reply_settings, source, withheld, attachments))


def fetchTweets():
    next_token = ""
    start_time = time.time()

    while True:
        # check request limit
        while time.time() - start_time < 3.1:
            time.sleep(0.3)

        start_time = time.time()
        # Credentials stored as environmental variables
        consumer_key = os.environ.get("APIKey")
        consumer_secret = os.environ.get("APIKeySecret")
        QUERY = '(CDU OR CSU OR SPD OR "DIE GRÜNEN" OR B90 OR LINKE OR FDP OR AFD OR Scholz OR Baerbock OR Laschet OR Lindner OR Weidel) lang:de -is:retweet'
        START_TIME = "2021-03-25T00:00:00Z"
        END_TIME = "2021-03-26T06:23:50.000Z"
        MAX_RESULTS = 100
        EXPANSIONS = 'author_id,geo.place_id'
        TWEET_FIELDS = 'created_at,author_id,public_metrics,context_annotations,entities,conversation_id,attachments,reply_settings,source,withheld'
        USER_FIELDS = 'created_at,description,entities,name,username,location,verified,public_metrics,withheld,protected'


        #Authentication with Credentials
        api = TwitterAPI(consumer_key, consumer_secret, auth_type="oAuth2", api_version='2')

        search_params = {
                'query': {QUERY},
                'start_time': START_TIME,
                'end_time': END_TIME,
                'max_results': MAX_RESULTS,
                'expansions': EXPANSIONS,
                'tweet.fields': TWEET_FIELDS,
                'user.fields': USER_FIELDS,
            }
        # print(search_params)
        
        if next_token:
            search_params['next_token'] = next_token

        # request data
        req = api.request('tweets/search/all', search_params)
        
        
        # for each result: insert user if not exists and insert tweet into db
        for twresult in req:     
            insertTweet(twresult['id'], twresult['text'], twresult['created_at'], twresult['author_id'], str(twresult.get("context_annotations", "")), str(twresult.get("entities", "")), twresult['conversation_id'], twresult['public_metrics']['retweet_count'], twresult['public_metrics']['reply_count'], twresult['public_metrics']['like_count'], twresult['public_metrics']['quote_count'], twresult['reply_settings'], twresult.get('source', ""), str(twresult.get('withheld', "")), str(twresult.get('attachments', "")))
            
        metaData = req.json()['meta']

        # get the next_token if more data is available (get next page of results)
        if 'next_token' in metaData:
            next_token = metaData['next_token']
        else:
            print("keine weiteren Daten verfügbar")
            break
        print(req.get_quota())


db()
fetchTweets()

con.close()

print("Tweets fetched")