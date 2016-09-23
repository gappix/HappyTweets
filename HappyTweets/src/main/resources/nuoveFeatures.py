from tweepy import StreamListener   
import tweepy, time
import socket
import sys


def initialize():


    #chiavi di accesso twitter (valori di BIANCINI)
    consumer_key =      'hARrkNBpwsh8lLldQt7fTe4iM'
    consumer_secret =   'p0BRXCYEePUrJXPHQBxdIkP14idAYaSi934VJU2Hm2LBCUuqg0'
    key =               '72019464-oUaReZ3i91fcVKg3Y7mBOzxlNrNXMpa5sxOcIld3R'
    secret =            '338I4ldbMc3CDpGYrpx5BuDfYbcAAZbJRDW86i9EY6Nwf'


    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(key, secret)
    api = tweepy.API(auth)

    index = 0

    while index<10 :

        listen = TwitterStreamListener(api)
        
        #while 1:

        stream = tweepy.Stream(auth, listen)

        try:
            stream.filter(languages=["it"])

        except:
            print >>sys.stderr, 'FUUUUUUUUUUUUUCK! Error by Tweepy'

        index +=1
            

    






class TwitterStreamListener(StreamListener):
    
    #costruttore
    def __init__(self, something):

        StreamListener.__init__(self)
        print >>sys.stderr, 'ciao sto iniziando a fare cose' 
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print >>sys.stderr , 'ciao, ho fatto una cosa'
        server_address = ('localhost', 9998)
        print >> sys.stderr, 'ciao, ho fatto una cosa e ci ho preso gusto cosi ne ho fatta un.altra'
        self.sock.connect(server_address)
        print >> sys.stderr, 'a furia di fare cose mi sono rotto, tanti ssssaluti!'


    def __del__ (self):

        if hasattr(self,'sock'):
            print >> sys.stderr, 'ciao, mi stavi simpatico, e stato bello!'
            self.sock.close()


    def on_data(self, data):

        #text_i = data.find("text")
        #source_i = data.find("source")
        #text = data[text_i + 8: source_i].decode('utf_8')
        
        
        
        print >> sys.stderr, 'connecting to  port' 
        
        
        try:
            print >>sys.stderr, 'sending "%s"' % data
            self.sock.send(data)
        
        except:
            print >>sys.stderr, 'error by socket'

            self.__del__()
            self.__init__()
            raise


        finally:
            print >>sys.stderr, 'closing socket'
            #sock.close()




    def on_error(self, status_code):
        if status_code == 420:
            return False


    def on_limit(self, track):
        sys.stderr.write(track + "\n")
        return

    def on_error(self, status_code):
        sys.stderr.write('Error: ' + str(status_code) + "\n")
        return False

    def on_timeout(self):
        sys.stderr.write("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return 



if __name__ == '__main__':
    initialize()
