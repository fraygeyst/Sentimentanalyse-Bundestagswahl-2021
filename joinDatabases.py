#########################
### Join all databases to one central database
#
### Requirements:
#	 -database with the fetched tweets
#	 -database with metadata from dbuserMeta.db
#	 -database with the analysed sentiment 
#	 -database with the buzzword coding
#	 -a new database to store everything there
#	 -sqlite3 module to connect to the database files
#	 -pandas to read the query and merge the columns together
#
### Results:
#	 -database dbjoinedData.db with alle data on one central place
# 	 -final structure:
#		*from dbTweets.db: tweet_id, created_at, author_id, conversation_id, 
# 						   retweet_count, reply_count, like_count, quote_count, reply_settings, source
#		*from dbuserMeta.db: tweet_id, media, company, politician
# 		*from dbsentiment.db: tweet_id, sentiment
# 		*from dbBuzzwords.db: tweet_id, cdu, csu, spd, gruene, fdp, afd, linke, laschet, scholz, baerbock, lindner, weidel
#		*withheld, attachements, tweet_text, contextannotations are not interesting for SAP Analytics Cloud Dashboarding
#	 -key: tweet_id
#	 -csv file with the result for SAP Analytics Cloud
#
#########################

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


#########################
### Function joinData() 
# 
# - creates for every database a dataframe
# - merges the dataframes in 3 steps with an inner join
# - The selects of df2 and df4 exclude tweet_text, which is included in df1 already
# - the key for all joins is "tweet_id"
# - the dataframe, created for the join connects via sql to the joinedData table of dbjoinedData.db
# - the result will be exported as a csv as well to use it in SAP Analytics Cloud
# 
#########################

def joinData():
	df1 = pd.read_sql_query("SELECT tweet_id, created_at, author_id, conversation_id, retweet_count, reply_count, like_count, quote_count, reply_settings, source FROM Tweet", con) 
	# insert tweet_text here for topic model
	df2 = pd.read_sql_query("SELECT tweet_id, media, company, politician FROM data", con2) 
	df3 = pd.read_sql_query("SELECT * FROM data", con3)
	# comment out this next line to create a new version for the topic model
	df4 = pd.read_sql_query("SELECT tweet_id, cdu, csu, spd, gruene, fdp, afd, linke, laschet, scholz, baerbock, lindner, weidel FROM buzzwords", con4)

	df = pd.merge(df1,df2, how="inner", on="tweet_id")
	df = pd.merge(df,df3, how="inner", on="tweet_id")
	df = pd.merge(df,df4, how="inner", on="tweet_id")

	df.to_sql(name='joinedData', con=con5, index=False)
	df.to_csv (r'C:\Users\marc_\OneDrive\Desktop\joinedData.csv')

joinData()


###DEBUG
print("Data joined")