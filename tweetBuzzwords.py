import sqlite3


con = sqlite3.connect("dbTweets.db")
con2 = sqlite3.connect("dbBuzzwords.db")
cur = con.cursor()
cur2 = con2.cursor()


con2.commit()

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



	print("start adding buzzwords")
	
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

print("buzzwords added")