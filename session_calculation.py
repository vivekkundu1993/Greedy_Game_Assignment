
import pymysql
import json
connection_mysql = pymysql.connect(host='localhost', user='root', db='Greedy_Game', password='root',init_command='SET NAMES UTF8')   #local mysql connection
cursor_mysql = connection_mysql.cursor()  #mysql cursor

import datetime

import operator
import time
class Headers:
    def __init__(self, ai5, debug, random, sdkv):
        self.ai5 = ai5
        self.debug = debug
        self.random = random
        self.sdkv = sdkv
    @staticmethod
    def get_headers(json_string):
        json_ = json.loads(json_string)
	#print(json_)
	#print((**json_))
        return Headers(**json_)

class Post:
    def __init__(self, event, ts):
        self.event = event
        self.ts = ts
    @staticmethod
    def get_post(json_string):
        json_ = json.loads(json_string)
	#print(json_)
	        
	return Post(**json_)  

class Bottle:
    def __init__(self, timestamp, game_id):
        self.timestamp = timestamp
        self.game_id = game_id
        self.timestamp_in_seconds = to_sec(timestamp)
    @staticmethod
    def get_bottle(json_string):
        json_ = json.loads(json_string)
        return Bottle(**json_)

class Instance:
    def __init__(self, headers, post, bottle):
        self.headers = headers
        self.post = post
        self.bottle = bottle
    @staticmethod
    def get_instance(json_string):
        json_ = json.loads(json_string)
        headers = Headers.get_headers(json.dumps(json_["headers"]))
	#print( headers)       
	post = Post.get_post(json.dumps(json_["post"]))
	
	#print( post)        
	bottle = Bottle.get_bottle(json.dumps(json_["bottle"]))
        return Instance(headers, post, bottle)

class Session:
    def __init__(self, ai5, val_sessions, tot_sessions, avg_session_time):
        self.ai5 = ai5
        self.val_sessions = val_sessions
        self.tot_sessions = tot_sessions
        self.avg_session_time = avg_session_time
    def to_json(self):
        return json.dumps(self.__dict__)

def read_json():
    user_map = {}
    with open('/home/vivek/Desktop/Greedy_Game_Assignment/ggevent.log') as infile:
        file = infile.readlines()
    for line in file:
        instance = Instance.get_instance(line)
	#print(instance.headers.ai5)
        users_instances = user_map.get(instance.headers.ai5, [])
	#print(users_instances)        
	users_instances.append(instance)
        user_map[instance.headers.ai5] = users_instances
    #print(user_map[instance.headers.ai5])
    #print(user_map)
    return user_map

def gaming_sessions(user_ins):
    sessions = []
    for ai5, instances in user_ins.items():
        
        flag = True
        start = None  
        prev = None  
        end = None  
        tot_sessions = 0  
        val_sessions = 0  
        total_time = 0  
        for instance in instances:
            if flag:
                if instance.post.event == "ggstart":
                    prev = start = instance
                    flag = False
                continue
            diff = instance.bottle.timestamp_in_seconds - prev.bottle.timestamp_in_seconds
            
            if prev.post.event == "ggstart" and instance.post.event == "ggstop":
                prev = end = instance
            elif diff > 30:  
                if prev.post.event == instance.post.event == "ggstop":
                    flag = True
                elif prev.post.event == "ggstop" and instance.post.event == "ggstart":
                    prev = start = instance
                elif prev.post.event == "ggstart" and instance.post.event == "ggstart":
                    prev = start = instance
                if end:
                    session_time = end.bottle.timestamp_in_seconds - start.bottle.timestamp_in_seconds
                
                if session_time >= 60:

                    val_sessions += 1
                    total_time += session_time
                
                if session_time > 1:
                    tot_sessions += 1

            if val_sessions !=0:
                average_session_time= total_time/val_sessions
            else:
                average_session_time=0
        sessions.append(
            Session(ai5, val_sessions, tot_sessions,average_session_time))
    return sessions
    

def to_sec(time_):
    datetime_format = "%Y-%m-%d %H:%M:%S.%f"
    strptime = datetime.datetime.strptime(time_, datetime_format)
    return int(time.mktime(strptime.timetuple()))

if __name__ == '__main__':
    user_ins = read_json()
    sessions = gaming_sessions(user_ins)
    with open('session_calculation_output_vivek.txt', "w") as fp:
        for session in sessions:
	    #print(session)
            fp.write(session.to_json() + "\n")
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
		#print(insert_query)
		cursor_mysql.execute(insert_query)
		connection_mysql.commit()
