#########################
### Clean tweets from noise like bots or retweets
#
### Requirements:
#	 - database dbTweets.db with tweets
#	 - sqlite module to connect to the database files
# 	 - detected bots 
# 	 
### Notices:
#	 - retweets were excluded at fetching, only for security purposes 
#	 - bots have to be detected by control
#	 - the same tweet by the same user more than 5 times will be excluded
#	 - tweets that are withheld in any country 
#
### Results:
#	 - cleaner data with less noise
#	 - 61.842 Tweets removed (06.12.2021) 
#
#########################

import sqlite3

con = sqlite3.connect("dbTweets.db")
cur = con.cursor()


def deleteNoise():
	with con:

		# delete retweets
		cur.execute("""DELETE FROM Tweet WHERE tweet_text LIKE 'RT%' """)

		# delete Bot
		cur.execute("""DELETE FROM Tweet WHERE author_id = XXXXXXX""" )

		# delete tweets with the same tweet_text posted by the same user > 5 times
		cur.execute("""DELETE FROM Tweet WHERE rowid IN 
			(SELECT rowid from Tweet where tweet_id IN 
			(select tweet_id from tweet, (SELECT tweet_text as tt, author_id as aid FROM Tweet GROUP BY tweet_text, author_id having count(*) > 5)
			where author_id = aid and tweet_text = tt)
			EXCEPT 
			SELECT MIN(rowid) FROM Tweet GROUP BY tweet_text, author_id having count(*) > 5)""")

		# delete tweets with less than 4 words
		cur.execute("""DELETE FROM Tweet WHERE tweet_id IN (SELECT tweet_id FROM Tweet WHERE length(tweet_text) - length(replace(tweet_text, ' ', '')) + 1 < 4)""")

		# delete tweets that are withheld in any country
		cur.execute("""DELETE FROM Tweet WHERE withheld LIKE 'copyright' """)


deleteNoise()
con.close()

###DEBUG
print("data cleaned")

