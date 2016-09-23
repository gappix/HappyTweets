from slistener2 import TwitterStreamListener  #import di un oggetto singolo (ovvero una classe)
import time, tweepy, sys		#import di tutta la libreria

# authentication 

#chiavi di accesso twitter (valori di BIANCINI)
consumer_key = 'hARrkNBpwsh8lLldQt7fTe4iM'
consumer_secret = 'p0BRXCYEePUrJXPHQBxdIkP14idAYaSi934VJU2Hm2LBCUuqg0'
key = '72019464-oUaReZ3i91fcVKg3Y7mBOzxlNrNXMpa5sxOcIld3R'
secret = '338I4ldbMc3CDpGYrpx5BuDfYbcAAZbJRDW86i9EY6Nwf'


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(key, secret)
api = tweepy.API(auth)



def main():


    listen = TwitterStreamListener(api)
    stream = tweepy.Stream(auth, listen)

	
    print ("Streaming started...")

	
    try: 
        stream.filter(locations=[-124.26,32.73,-114.00,41.96])
		#prima era [-6.38,49.87,1.77,55.81]
    except:
        print ("error!")
        stream.disconnect()

		
		
if __name__ == '__main__':
    main()
