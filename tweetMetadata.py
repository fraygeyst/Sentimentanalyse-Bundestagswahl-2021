# add metadata as topic prevalence covariates
import csv
import sqlite3
import re


con = sqlite3.connect("dbTweets.db")
con2 = sqlite3.connect("dbuserMeta.db")
cur = con.cursor()
cur2 = con2.cursor()


con2.commit()

def add_covariates():

	with con2:
		cur2.execute("CREATE TABLE IF NOT EXISTS data (tweet_text TEXT, tweet_id INTEGER, media INTEGER, company INTEGER, politician INTEGER)")
		cur2.execute("""CREATE INDEX IF NOT EXISTS "index1" ON data ("tweet_id")""")
	with con:
		result = cur.execute("SELECT tweet_text, tweet_id from Tweet").fetchall()


	tweets = []
	for req in result:
		twresult = req[0]
		#print(req[0])

		# remove hyperlinks
		twresult = re.sub("http[^\s]+", "", twresult)
		# remove any words starting with "@"
		twresult = re.sub("@#?[^\s]+", "", twresult)
		# remove special character
		twresult = re.sub("&amp[^\s]+", "", twresult)
		#remove hashtag symbol
		twresult = twresult.replace("#","")
		#remove newline
		twresult = twresult.replace("\n", " ")

		tweets.append((twresult, req[1], -1, -1, -1))


	with con2:
		cur2.executemany("INSERT INTO data VALUES (?,?,?,?,?)", tweets)



	print("start adding meta data")
	

	media_handelsblatt = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 5776022""").fetchall()
	media_faz = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 18016521""").fetchall()
	media_SZ = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 17803524 OR author_id = 19767324""").fetchall() # SZ Top-News, SZ Digital
	media_diezeit = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 1081459807""").fetchall()
	media_bild = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 9204502 OR author_id = 35707087""").fetchall() # BILD, BILD Digital
	media_t3n = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 11060982""").fetchall()
	media_heise = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 3197921""").fetchall()


	for m in media_handelsblatt:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_faz:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_SZ:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_diezeit:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_bild:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_t3n:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_heise:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])

	con2.commit()


	with open('wikidata_companies_dach.csv', 'r') as file:
	    reader = csv.reader(file)
	    for row in reader:
	        company_tweets = cur.execute("""SELECT tweet_id from Tweet WHERE author_id = '%s'""" % row[3]).fetchall()
	        for comp in company_tweets:
	            #tweets.append(comp[0])
	            cur2.execute("""UPDATE data SET company=1 WHERE tweet_id= '%s'""" % comp[0])
	con2.commit()

	with open('wikidata_politicans.csv', 'r') as file:
	    reader = csv.reader(file)
	    for row in reader:
	        politicians_tweets = cur.execute("""SELECT tweet_id from Tweet WHERE author_id = '%s'""" % row[3]).fetchall()
	        for tweet in politicians_tweets:
	            #tweets.append(tweet[0])
	            cur2.execute("""UPDATE data SET politician=1 WHERE tweet_id= '%s'""" % tweet[0])
	con2.commit()


	cur2.execute("""UPDATE data SET media = 0 WHERE media = -1""")
	cur2.execute("""UPDATE data SET company = 0 WHERE company = -1""")
	cur2.execute("""UPDATE data SET politician = 0 WHERE politician = -1""")
	con2.commit()

add_covariates()

con2.close()

print("metadata added")
