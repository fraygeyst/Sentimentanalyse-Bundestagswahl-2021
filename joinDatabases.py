import sqlite3
import pandas as pd

con = sqlite3.connect("dbTweets.db")
con2 = sqlite3.connect("dbuserMeta.db")
con3 = sqlite3.connect("dbSentiment.db")
con4 = sqlite3.connect("dbBuzzwords.db")
con5 = sqlite3.connect("dbjoinedData.db")
cur = con.cursor()
cur2 = con2.cursor()
cur3 = con3.cursor()
cur4 = con4.cursor()
cur5 = con5.cursor()


def joinData():
	df1 = pd.read_sql_query("SELECT tweet_id, created_at, author_id, context_annotations, entities, conversation_id, retweet_count, reply_count, like_count, quote_count, reply_settings, source, withheld, attachments FROM Tweet", con) 
	df2 = pd.read_sql_query("SELECT * FROM data", con2)
	df3 = pd.read_sql_query("SELECT * FROM data", con3)
	df4 = pd.read_sql_query("SELECT tweet_id, cdu, csu, spd, gruene, fdp, afd, linke, laschet, scholz, baerbock, lindner, weidel FROM buzzwords", con4)



	df = pd.merge(df1,df2, how="inner", on="tweet_id")
	df = pd.merge(df,df3, how="inner", on="tweet_id")
	df = pd.merge(df,df4, how="inner", on="tweet_id")

	df.to_sql(name='joinedData', con=con5, index=False)

joinData()
print("Data joined")