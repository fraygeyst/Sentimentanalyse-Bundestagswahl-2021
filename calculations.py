import pandas as pd
import sqlite3
import ast


conn = sqlite3.connect("dbTweets.db")
c = conn.cursor()


hashtags = {}
sqlQuery = "SELECT * FROM Tweet"
df = pd.read_sql_query(sqlQuery, conn)
df.created_at = pd.to_datetime(df.created_at)
df = df.set_index("created_at")
dftmp = df
for index, row in dftmp.iterrows():
    if row['entities']:
        if "'hashtags':" in row['entities']:
            entities = ast.literal_eval(row['entities'])
            for hashtag in entities['hashtags']:
                hashtag_lower = hashtag['tag'].lower()
                if hashtag_lower in hashtags:
                    hashtags[hashtag_lower] += 1
                else:
                    hashtags[hashtag_lower] = 1

#print(hashtags)

s_hashtags = {k: v for k, v in sorted(hashtags.items(), key=lambda item: item[1], reverse=True)}
s_hashtags_list = list(s_hashtags.items())

for i in range(0,50):
    print(i+1, ":\t",s_hashtags_list[i][1],  "\t", s_hashtags_list[i][0],)