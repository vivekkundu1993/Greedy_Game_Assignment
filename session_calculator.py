import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import time                           #importing all the required packageds/dependencies
import pymysql
import json
import datetime
import operator

#connection_mysql = pymysql.connect(host='localhost', user='*#*', db='Greedy_Game', password='*#*',init_command='SET NAMES UTF8')   #local mysql connection to store the user session data into mysql 


#cursor_mysql = connection_mysql.cursor()  #mysql cursor


class Event_Headers:
    def __init__(self, ai5, debug, random, sdkv):
        self.ai5 = ai5
        self.debug = debug
        self.random = random
        self.sdkv = sdkv
    @staticmethod
    def get_headers(json_string):
        json_ = json.loads(json_string)
        return Event_Headers(**json_)

class Event_Post:
    def __init__(self, event, ts):
        
	self.event = event
        self.ts = ts

    @staticmethod
    def get_post(json_input):
        
	json_ = json.loads(json_input)
	#print(json)
	        
	return Event_Post(**json_)  




class Event_Bottle:
    def __init__(self, timestamp, game_id):
        
	self.timestamp = timestamp
        self.game_id = game_id
        self.timestamp_in_seconds = timestamp_second_converter(timestamp)
    @staticmethod
    def get_bottle(json_input):
        json_ = json.loads(json_input)
        return Event_Bottle(**json_)

def timestamp_second_converter(input_time):                        #function to convert datestamp to seconds
    datetime_format = "%Y-%m-%d %H:%M:%S.%f"
    
    dt = datetime.datetime.strptime(input_time, datetime_format)
    return int(time.mktime(dt.timetuple())) 





class Instance:
    def __init__(self, headers, post, bottle):
        self.headers = headers
        self.post = post
        self.bottle = bottle
    @staticmethod
    def get_instance(json_input):
        json_ = json.loads(json_input)
        headers = Event_Headers.get_headers(json.dumps(json_["headers"]))
	#print( headers)       
	post = Event_Post.get_post(json.dumps(json_["post"]))
	
	#print( post)        
	bottle = Event_Bottle.get_bottle(json.dumps(json_["bottle"]))
        return Instance(headers, post, bottle)

class Session:
    def __init__(self, ai5, val_sessions, tot_sessions, avg_session_time):
        self.ai5 = ai5
        self.val_sessions = val_sessions
        self.tot_sessions = tot_sessions
        self.avg_session_time = avg_session_time
    def to_json(self):
        return json.dumps(self.__dict__)


def user_session_calculator(user_ins):                              #function to calculate user Session  
    user_session = []
    for ai5, instances in user_ins.items():
        count=0 
        flag = 1
        
	start_session_instance = None  
        prev_session_instance = None  
        end_session_instance = None  
        
	total_sessions = 0  
        valid_sessions = 0  
        total_time = 0
	 
        for instance in instances:
            count+=1
            
	    if count==len(instances):
		if start_session_instance is not None and instance.post.event == "ggstop":
			session_time=instance.bottle.timestamp_in_seconds-start_session_instance.bottle.timestamp_in_seconds
		if session_time >= 60:
		    #print(session_time)
                    valid_sessions += 1
                    total_time += session_time
                
                if session_time > 1:
                    total_sessions += 1
	    if flag:
                if instance.post.event == "ggstart":
                    prev_session_instance = start_session_instance = instance
                    flag = 0
                continue
            diff = instance.bottle.timestamp_in_seconds - prev_session_instance.bottle.timestamp_in_seconds
            #print(diff)
            if prev_session_instance.post.event == "ggstart" and instance.post.event == "ggstop":
                prev_session_instance = end_session_instance = instance
            elif diff > 30:  
                
                
                if end_session_instance and instance.post.event == "ggstart":
                    session_time = end_session_instance.bottle.timestamp_in_seconds - start_session_instance.bottle.timestamp_in_seconds
                    #print(session_time, end_session_instance.bottle.timestamp,start_session_instance.bottle.timestamp)
		    prev_session_instance = start_session_instance = instance
		    
                if session_time >= 60:
		    
                    valid_sessions += 1
                    total_time += session_time
                
                if session_time > 1:
                    total_sessions += 1
	    elif (diff<=30)   :
		 end_session_instance =prev_session_instance=instance

            if valid_sessions !=0:
            	average_session_time= total_time/valid_sessions
            else:
                average_session_time=0
        user_session.append(
            Session(ai5, valid_sessions, total_sessions,average_session_time))
    return user_session



def json_read():                                   #function to read the input JSON file
    user_dict = {}                                  #dictionary declaration to store user events as a key value pair
    with open('/home/vivek/Desktop/Greedy_Game_Assignment/samplegg.log') as f:      #file opening statement from given location
        json_data = f.readlines()
    
    for line in json_data:
        event = Instance.get_instance(line)                                
	#print(instance.headers.ai5)
        user_event = user_dict.get(event.headers.ai5, [])
	#print(users_instances)        
	user_event.append(event)
        user_dict[event.headers.ai5] = user_event                                #saving each instance corresponding to its key ie. ai5
    #print(user_map[instance.headers.ai5])
    
    return user_dict










    
if __name__ == '__main__':

	user_event_info = json_read()                                  #calling read_json function to read the input log file (json format) 
	user_sessions_info = user_session_calculator(user_event_info)            #calling the user_session_calculator method to calculate totalsession,validsession,avg session of each user by giving user's event history as input
	with open('session_calculation_output_vivek.txt', "w") as f:
		for i in user_sessions_info:
		    #print(session)
		    f.write(i.to_json() + "\n")
	
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
	
