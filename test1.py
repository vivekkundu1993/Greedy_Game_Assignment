import ast
# Read all lines into a list
count=1
import pymysql
import json
connection_mysql = pymysql.connect(host='localhost', user='root', db='Greedy_Game', password='root',init_command='SET NAMES UTF8')
cursor_mysql = connection_mysql.cursor()
query="show tables;"


with open('ggevent.log', 'r') as f:
    for line in f:
	

	a=json.loads(line)
	headers=a["headers"]
	post=a["post"]
	bottle=a["bottle"]
	
	
	ai5=headers["ai5"]
	sdkv=headers["sdkv"]
	event=post["event"]
	ts=post["ts"]	
	timestamp=bottle["timestamp"]
	game_id=bottle["game_id"]	
	#print(ai5,sdkv,game_id,timestamp,ts,event)
	count+=1
	#print ( timestamp)
	insert_query="insert into GgeventLog (ai5 , sdkv  , event , game_id  , timestamp, ts) values ('" + str(ai5) + "' , '" + str(sdkv) + "' , '" +  str(event) + "' , " + str(game_id) +", '" + str(timestamp) + "', '" + str(ts) + "')"
	cursor_mysql.execute(insert_query)
connection_mysql.commit()


