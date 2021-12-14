#########################
### Check every tweet for buzzwords from search QUERY
#
### Requirements:
#	 -fetched Tweet database dbTweets.db
# 	 -creation of new database dbBuzzwords.db with table buzzword
# 	 -sql module to access database
# 	 
### Results:
#	 -database dbBuzzwords.db with table buzzwords
#	 -column for every buzzword from search query
#	 	(cdu, csu, spd, gruene, fdp, afd, linke, laschet, scholz, baerbock, lindner, weidel)
#	 -possibility to see, which buzzword is in a tweet and which one is not
#
#########################

import sqlite3

con = sqlite3.connect("dbTweets.db")
con2 = sqlite3.connect("dbBuzzwords.db")
cur = con.cursor()
cur2 = con2.cursor()

con2.commit()


#########################
### Function add_buzzwords() / 1st block
# 
# -Creation of the new database with a new table if not existing
# 		with tweet_id as key
# -search request trough all tweet_text of Tweets 
# -insert 12 columns with an int -1 to validate in further steps and have a control value 
#		(if there will be a -1 be left, there is an error)
#  
#########################

def add_buzzwords():

	with con2:
		cur2.execute("CREATE TABLE IF NOT EXISTS buzzwords (tweet_text TEXT, tweet_id INTEGER, cdu INTEGER, csu INTEGER, spd INTEGER, gruene INTEGER, fdp INTEGER, afd INTEGER, linke INTEGER, laschet INTEGER, scholz INTEGER, baerbock INTEGER, lindner INTEGER, weidel INTEGER)")
		cur2.execute("""CREATE INDEX IF NOT EXISTS "index1" ON buzzwords ("tweet_id")""")
	with con:
		result = cur.execute("SELECT tweet_text, tweet_id from Tweet").fetchall()

	tweets = []
	for req in result:
		twresult = req[0]
		tweets.append((twresult, req[1], -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1))

	with con2:
		cur2.executemany("INSERT INTO buzzwords VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tweets)


	### DEBUG
	print("start adding buzzwords")
	

	#########################
	### Function add_buzzwords() / 2nd block
	#
	# -every tweet is controlled, if there is a search query in tweet_text
	# -if it is the case, the tweet id will be saved in a variable
	# -all data whith a hit will get a 1 instead of the -1
	# -in the last step, every -1 will be replaced with 0
	#
	#########################

	cdu = cur.execute("""SELECT tweet_id FROM Tweet WHERE tweet_text LIKE '%cdu%' """).fetchall()
	csu = cur.execute("""SELECT tweet_id FROM Tweet WHERE tweet_text LIKE '%csu%' """).fetchall()
	spd = cur.execute("""SELECT tweet_id FROM Tweet WHERE tweet_text LIKE '%spd%' """).fetchall()
	gruene = cur.execute("""SELECT tweet_id FROM Tweet WHERE tweet_text LIKE '%gruene%' OR tweet_text LIKE '%grüne%' OR tweet_text LIKE '%b90%' """).fetchall()
	fdp = cur.execute("""SELECT tweet_id FROM Tweet WHERE tweet_text LIKE '%fdp%' """).fetchall()
	afd = cur.execute("""SELECT tweet_id FROM Tweet WHERE tweet_text LIKE '%afd%' """).fetchall()
	linke = cur.execute("""SELECT tweet_id FROM Tweet WHERE tweet_text LIKE '%linke%' """).fetchall()
	laschet = cur.execute("""SELECT tweet_id FROM Tweet WHERE tweet_text LIKE '%laschet%' """).fetchall()
	scholz = cur.execute("""SELECT tweet_id FROM Tweet WHERE tweet_text LIKE '%scholz%' """).fetchall()
	baerbock = cur.execute("""SELECT tweet_id FROM Tweet WHERE tweet_text LIKE '%baerbock%' """).fetchall()
	lindner = cur.execute("""SELECT tweet_id FROM Tweet WHERE tweet_text LIKE '%lindner%' """).fetchall()
	weidel = cur.execute("""SELECT tweet_id FROM Tweet WHERE tweet_text LIKE '%weidel%' """).fetchall()

	for m in cdu:
		cur2.execute("""UPDATE buzzwords SET cdu=1 WHERE tweet_id= '%s'""" % m[0])
	for m in csu:
		cur2.execute("""UPDATE buzzwords SET csu=1 WHERE tweet_id= '%s'""" % m[0])
	for m in spd:
		cur2.execute("""UPDATE buzzwords SET spd=1 WHERE tweet_id= '%s'""" % m[0])
	for m in gruene:
		cur2.execute("""UPDATE buzzwords SET gruene=1 WHERE tweet_id= '%s'""" % m[0])
	for m in fdp:
		cur2.execute("""UPDATE buzzwords SET fdp=1 WHERE tweet_id= '%s'""" % m[0])
	for m in afd:
		cur2.execute("""UPDATE buzzwords SET afd=1 WHERE tweet_id= '%s'""" % m[0])
	for m in linke:
		cur2.execute("""UPDATE buzzwords SET linke=1 WHERE tweet_id= '%s'""" % m[0])
	for m in laschet:
		cur2.execute("""UPDATE buzzwords SET laschet=1 WHERE tweet_id= '%s'""" % m[0])
	for m in scholz:
		cur2.execute("""UPDATE buzzwords SET scholz=1 WHERE tweet_id= '%s'""" % m[0])
	for m in baerbock:
		cur2.execute("""UPDATE buzzwords SET baerbock=1 WHERE tweet_id= '%s'""" % m[0])
	for m in lindner:
		cur2.execute("""UPDATE buzzwords SET lindner=1 WHERE tweet_id= '%s'""" % m[0])
	for m in weidel:
		cur2.execute("""UPDATE buzzwords SET weidel=1 WHERE tweet_id= '%s'""" % m[0])

	con2.commit()

	cur2.execute("""UPDATE buzzwords SET cdu = 0 WHERE cdu = -1""")
	cur2.execute("""UPDATE buzzwords SET csu = 0 WHERE csu = -1""")
	cur2.execute("""UPDATE buzzwords SET spd = 0 WHERE spd = -1""")
	cur2.execute("""UPDATE buzzwords SET gruene = 0 WHERE gruene = -1""")
	cur2.execute("""UPDATE buzzwords SET fdp = 0 WHERE fdp = -1""")
	cur2.execute("""UPDATE buzzwords SET afd = 0 WHERE afd = -1""")
	cur2.execute("""UPDATE buzzwords SET linke = 0 WHERE linke = -1""")
	cur2.execute("""UPDATE buzzwords SET laschet = 0 WHERE laschet = -1""")
	cur2.execute("""UPDATE buzzwords SET scholz = 0 WHERE scholz = -1""")
	cur2.execute("""UPDATE buzzwords SET baerbock = 0 WHERE baerbock = -1""")
	cur2.execute("""UPDATE buzzwords SET lindner = 0 WHERE lindner = -1""")
	cur2.execute("""UPDATE buzzwords SET weidel = 0 WHERE weidel = -1""")
	con2.commit()

add_buzzwords()
con2.close()

### DEBUG message to show add_buzzwords() is done and con2 closed
print("buzzwords added")