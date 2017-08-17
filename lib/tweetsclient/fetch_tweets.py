from datetime import datetime
from time import sleep
import anyjson
import heapq
import tweepy
import logbook

_script_ = (os.path.basename(__file__)
            if __name__ == "__main__"
            else __name__)
log = logbook.Logger(_script_)

def fetch(api, callback, items, conn):
    cursor = conn.cursor()
    items = items

    def max_heap(items):
        new_items, sorted_items = [], []
        now = datetime.now()
        for item in items:
            screen_name = item[1]
            updated_at = items[0]
            q = "SELECT  id, created FROM tweets WHERE user_name='%s' ORDER BY id ASC LIMIT 1" % screen_name
            res = cursor.execute(q)
            if res == 0:
                new_items.append((now - now, None, screen_name))
            elif res == 1:
                id, created = cursor.fetchone()
                new_items.append((now - created, id, screen_name))
        heapq.heapify(new_items)
        for i in range(len(new_items)):
            item = heapq.heappop(new_items)
            sorted_items.append([item[1], item[2]])
        return sorted_items
            
        
    
    def api_status():
        now = datetime.now()
        try:
            rate_limit_status = api.last_response.headers
            remaining, reset = "x-rate-limit-remaining", "x-rate-limit-reset"
            rate_limit_status[remaining], rate_limit_status[reset]
        except (AttributeError, KeyError) as e:
            rate_limit_status = api.rate_limit_status()['resources']['statuses']['/statuses/user_timeline']
            remaining, reset = "remaining", "reset"
        rate_limit_remaining, rate_limit_reset = int(rate_limit_status[remaining]), datetime.fromtimestamp(float(rate_limit_status[reset]))
        seconds = (rate_limit_reset - now).seconds
        return 0 if rate_limit_remaining > 5 else seconds

    
        
    def batch_fetch(screen_name, oldest_id=None):
        sleep(api_status())
        new_tweets = api.user_timeline(screen_name=screen_name, count=200, max_id=oldest_id)

        l = len(new_tweets)
        
        while l > 0:
            for tweet in new_tweets:
                callback.add_to_queue(anyjson.serialize(tweet._json))

                oldest_id = new_tweets[-1].id - 1

                headers = api.last_response.headers

            sleep(api_status())
            new_tweets = api.user_timeline(screen_name=screen_name, count=200, max_id=oldest_id)
            l = len(new_tweets)
            
    while True:
        sorted_items = max_heap(items)
        for oldest_tweet, screen_name in sorted_items:
            oldest_tweet = oldest_tweet -1 if oldest_tweet else None
            try:
                batch_fetch(screen_name, oldest_tweet)
            except tweepy.error.TweepError as e:
                pass
        sleep(600)

             
            

