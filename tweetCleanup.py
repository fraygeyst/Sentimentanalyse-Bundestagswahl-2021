import sqlite3
from datetime import date

### Database ###

con = sqlite3.connect("dbTweets.db")
cur = con.cursor()



def deleteNoise():
	with con:
		#delete retweets
		cur.execute("""DELETE FROM Tweet WHERE tweet_text LIKE 'RT%' """)
		
		#delete Bot
		cur.execute("""DELETE FROM Tweet WHERE author_id = 2855732137""" )

		#delete tweets (ads) with the same tweet_text posted by the same user > 5 times
		cur.execute("""DELETE FROM Tweet WHERE rowid IN 
			(SELECT rowid from Tweet where tweet_id IN 
			(select tweet_id from tweet, (SELECT tweet_text as tt, author_id as aid FROM Tweet GROUP BY tweet_text, author_id having count(*) > 1)
			where author_id = aid and tweet_text = tt)
			EXCEPT 
			SELECT MIN(rowid) FROM Tweet GROUP BY tweet_text, author_id having count(*) > 1)""")

		#delete tweets with less than 4 words
		cur.execute("""DELETE FROM Tweet WHERE tweet_id IN (SELECT tweet_id FROM Tweet WHERE length(tweet_text) - length(replace(tweet_text, ' ', '')) + 1 < 4)""")


deleteNoise()

con.close()

print("data cleaned")

#61.527 Tweets removed (24.10.2021)