import sqlite3
from germansentiment import SentimentModel

con = sqlite3.connect("dbTweets.db")
con2 = sqlite3.connect("dbSentiment.db")
cur = con.cursor()
cur2 = con2.cursor()

with con2:
    cur2.execute("CREATE TABLE IF NOT EXISTS data (tweet_id INTEGER, sentiment TEXT)")
    cur2.execute("""CREATE INDEX IF NOT EXISTS "index1" ON data ("tweet_id")""")

# get tweets from DB
tweets = cur.execute("""SELECT tweet_id, tweet_text FROM Tweet""").fetchall()

# use model to predict sentiment ('oliverguhr/german-sentiment-bert')
model = SentimentModel()

# calculate sentiment in batches (100 tweets/batch)
for i in range(32205, 60849): # duration: ~7 days         #83, 1223, 1300, 2411, 3197, 7752, 8788, 9225, 9393, 12985, 13733, 15502, 17631, 22183, 23455,26876, 27920, 29275,30352, 30821, 30863,32062, 32204
    data = []
    batch_tweets = tweets[100 * (i - 1):100 * i]
    batch_texts = [t[1] for t in batch_tweets]
    result = model.predict_sentiment(batch_texts)

    for j in range(0, len(batch_texts)):
        data.append((batch_tweets[j][0], result[j]))

    with con2:
        cur2.executemany("INSERT INTO data VALUES (?,?)", data)
    print("Batch", i, "inserted.")