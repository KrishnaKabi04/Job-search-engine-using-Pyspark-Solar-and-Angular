import sys
import tweepy
import time
import json
from tweepy import OAuthHandler


api_key = ""
api_secret_key = ""
access_token =  ""
access_token_secret =  ""

authentication = tweepy.OAuthHandler(api_key, api_secret_key)

authentication.set_access_token(access_token, access_token_secret)

api = tweepy.API(authentication)

class MyStreamListener(tweepy.StreamListener):
    
    file_object = []
    def __init__(self, count, time_limit=7200):
        self.start_time = time.time()
        self.limit = time_limit
        super(MyStreamListener, self).__init__()
        self.file_object = open('output' + str(count) + '.json', 'w')
        self.file_object.write('[')
    
    def on_connect(self):
        print("Connected!")
        
    def on_data(self, data):
        try:

            data = json.loads(data)
            time.sleep(0.3)
            if data["lang"]=="en":
                self.file_object.write(str(json.dumps(data)))
                self.file_object.write(',')
            
            if (time.time() - self.start_time) > self.limit:
            
                print(time.time(), self.start_time, self.limit)
                self.file_object.write(']')
                self.file_object.close()
                return False
        except:
            self.file_object.write(']')
            self.file_object.close()

    def on_error(self, status_code):
        self.file_object.write(']')
        self.file_object.close()
        if status_code == 420:
            time.sleep(900)
            return False

count = 0
num_retries = 5
while count < num_retries:
    myStreamListener = MyStreamListener(count)
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener, tweet_mode="extended")
    myStream.filter(track=['hiring', 'recruitment'])
    count = count + 1