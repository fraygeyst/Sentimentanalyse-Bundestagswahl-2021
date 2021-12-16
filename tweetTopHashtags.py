#########################
### Generating a list with the most used hashtags
#
### Requirements: 
#   -database with fetched tweets
#   -entities row, where hashtags are included
#   -pandas module
#   -sqlite module to access database dbTweets
#   -ast module to process abstract syntax trees
#
### Results:
#   -Console log with the 50 hashtags, contained in fetched tweets
#   -stored in JSON and CSV ready format 
#   -final results stored in ./consolelogs/topHashtags.json
#
#########################

import pandas as pd
import sqlite3
import ast


#########################
### Variables and connections
# 
# Conneting to database dbTweets
# Dictionary with the hashtags from entities
# sqlQuery by selecting all data from Tweet table
# dataframe with panda
# 
#########################

con = sqlite3.connect("dbTweets.db")
c = con.cursor()
hashtags = {}
sqlQuery = "SELECT * FROM Tweet"
df = pd.read_sql_query(sqlQuery, con)


for index, row in df.iterrows():
    if row['entities']:
        if "'hashtags':" in row['entities']:
            entities = ast.literal_eval(row['entities'])
            for hashtag in entities['hashtags']:
                hashtag_lower = hashtag['tag'].lower()
                if hashtag_lower in hashtags:
                    hashtags[hashtag_lower] += 1
                else:
                    hashtags[hashtag_lower] = 1

### DEBUG
#print(hashtags)


#########################
### Results 
# 
# loop to print the hashtags from highest to lowest in console
# Formatting example: 
#   {"id": i, "count": /count of the hashtag/, "hashtag": "/hashtag/"},
#   last item does not need a comma in the end
#
#########################

s_hashtags = {k: v for k, v in sorted(hashtags.items(), key=lambda item: item[1], reverse=True)}
s_hashtags_list = list(s_hashtags.items())

###JSON Output
for i in range(0,49):
    print('{"id": ' + str(i+1) + ', "count": ' + str(s_hashtags_list[i][1]) + ', "hashtag": "' + s_hashtags_list[i][0] + '"},')
for i in range(49,50):
    print('{"id": ' + str(i+1) + ', "count": ' + str(s_hashtags_list[i][1]) + ', "hashtag": "' + s_hashtags_list[i][0] + '"}')


###CSV Output
print("id,count,hashtag")
for i in range(0,50):
    print(str(i+1) + ',' + str(s_hashtags_list[i][1]) + ',' + s_hashtags_list[i][0])