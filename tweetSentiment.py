#########################
### Using the Twitter API to fetch tweets
#
### Requirements: 
#   - database with tweets
#   - new database to store sentiment
#   - model to predict sentiment ('oliverguhr/german-sentiment-bert')
#
### Implementation:
#   - sentiment will be analyzed in 60.849 batches with 100 tweets per batch
#   - Compiling can be stopped at any position
#           Start batch has to be replaced with last compiled batch + 1 
#   - duration: ~8 days
#   - Batch where compiling was stopped manually: 83, 1223, 1300, 2411, 3197, 7752, 8788, 9225, 9393, 12985, 13733, 15502, 17631, 22183, 23455,26876, 27920, 29275,30352, 30821, 30863,32062, 32204, 32833 (abgebrochen), 36697, 37258, 37754, 40994,43966, 45125, 46399, 53652, 55003, 57161, 57167
# 
### Results:
#   - database dbSentiment.db with sentiment information
#           tweet_id and sentiment stored
#   - console log after each completed batch as feedback
#
#########################

import sqlite3
from germansentiment import SentimentModel

con = sqlite3.connect("dbTweets.db")
con2 = sqlite3.connect("dbSentiment.db")
cur = con.cursor()
cur2 = con2.cursor()

with con2:
    cur2.execute("CREATE TABLE IF NOT EXISTS data (tweet_id INTEGER, sentiment TEXT)")
    cur2.execute("""CREATE INDEX IF NOT EXISTS "index1" ON data ("tweet_id")""")

tweets = cur.execute("""SELECT tweet_id, tweet_text FROM Tweet""").fetchall()
model = SentimentModel()

for i in range(1, 60849):      
    data = []
    batch_tweets = tweets[100 * (i - 1):100 * i]
    batch_texts = [t[1] for t in batch_tweets]
    result = model.predict_sentiment(batch_texts)

    for j in range(0, len(batch_texts)):
        data.append((batch_tweets[j][0], result[j]))

    with con2:
        cur2.executemany("INSERT INTO data VALUES (?,?)", data)
    ###DEBUG
    print("Batch", i, "inserted.")