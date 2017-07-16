import ast
# Read all lines into a list
count=1
import pymysql
import json
connection_mysql = pymysql.connect(host='localhost', user='root', db='Greedy_Game', password='root',init_command='SET NAMES UTF8')
cursor_mysql = connection_mysql.cursor()
query="show tables;"


with open('/home/vivek/Desktop/Greedy_Game_Assignment/session_calculation_output_vivek.txt', 'r') as f:
    for line in f:
	user_info=json.loads(line)
	#print(a)
	#print(type(a))
	ai5=user_info["ai5"]
	tot_sessions=user_info["tot_sessions"]
	val_sessions=user_info["val_sessions"]
	avg_session_time=user_info["avg_session_time"]
	insert_query="insert into user_session_info (ai5 , total_sessions  , valid_sessions , average_session_time_inseconds ) values ('" + str(ai5) + "' , '" + str(tot_sessions) + "' , '" +  str(val_sessions) + "' ,'" + str(avg_session_time)  + "')"
	print(insert_query)
	cursor_mysql.execute(insert_query)
	connection_mysql.commit()
