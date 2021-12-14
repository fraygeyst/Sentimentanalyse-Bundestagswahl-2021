#########################
### Add metadata as topic prevalence covariates
#
### Requirements: 
#   - Tweets from dbTweets.db
#	- new database dbuserMeta.db
#	- wikidata.csv files for companies and politicians
#	- author id's of big german media companies
#
###Implementation:
#	- for topic modeling, the tweet_text's are cleaned up and every noise will be removed from the text
#	- all author_id's are checked, if there are listed in known data sets
#	- if the author_id is in the fields of media, politican or company, the dataset has a 1 in the suitable field
#	- otherwise a 0 is set
#
### Results:
#	- cleaned up tweet_text option for topic model
#   - database with information about author id origin 
#	- information if a author is listed as company, politican or media companie
#
#########################

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


# tweet text cleanup
	tweets = []
	for req in result:
		twresult = req[0]
		
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

	###DEBUG
	print("start adding meta data")
	

# media
	media_faz = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 18016521""").fetchall() #Frankfurter Allgemeine Zeitung
	media_SZ = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 17803524 OR author_id = 19767324""").fetchall() # SZ Top-News, SZ Digital
	media_diezeit = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 1081459807""").fetchall() #Die Zeit
	media_bild = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 9204502 OR author_id = 35707087""").fetchall() # BILD, BILD Digital
	media_saar = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 20316016""").fetchall() #Saarbrücker Zeitung
	media_thueringer = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 19865201""").fetchall() #Thüringer Allgemeine
	media_swpresse = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 18971587""").fetchall() #Südwest Presse
	media_ksta = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 23440299""").fetchall() #Kölner Stadtanzeiger
	media_augsburg = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 24150568""").fetchall() #Augsburger Allgemeine
	media_rp = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 15427879""").fetchall() #Rheinische Post
	media_stuttgarter = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 18552451""").fetchall() #Stuttgarter Zeitung
	media_wz = cur.execute("""SELECT tweet_id FROM Tweet WHERE author_id = 28314751""").fetchall() #Westdeutsche Zeitung

	for m in media_faz:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_SZ:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_diezeit:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_bild:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_saar:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_thueringer:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_swpresse:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_ksta:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_augsburg:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_rp:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_stuttgarter:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	for m in media_wz:
		cur2.execute("""UPDATE data SET media=1 WHERE tweet_id= '%s'""" % m[0])
	con2.commit()


# companies
	with open('wikidata_companies_dach.csv', 'r') as file:
		reader = csv.reader(file)
		for row in reader:
				company_tweets = cur.execute("""SELECT tweet_id from Tweet WHERE author_id = '%s'""" % row[3]).fetchall()
	for comp in company_tweets:
			cur2.execute("""UPDATE data SET company=1 WHERE tweet_id= '%s'""" % comp[0])
	con2.commit()


# politicans
	with open('wikidata_politicans.csv', 'r') as file:
		reader = csv.reader(file)
		for row in reader:
			politicians_tweets = cur.execute("""SELECT tweet_id from Tweet WHERE author_id = '%s'""" % row[3]).fetchall()
	for tweet in politicians_tweets:
			cur2.execute("""UPDATE data SET politician=1 WHERE tweet_id= '%s'""" % tweet[0])
	con2.commit()


cur2.execute("""UPDATE data SET media = 0 WHERE media = -1""")
cur2.execute("""UPDATE data SET company = 0 WHERE company = -1""")
cur2.execute("""UPDATE data SET politician = 0 WHERE politician = -1""")
con2.commit()


add_covariates()
con2.close()

###DEBUG
print("metadata added")
