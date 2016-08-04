package it.reti.spark.samples

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.net.Socket

class SocketReceiver(port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  var serverSocket : ServerSocket = null;
  
  def onStart() {
    serverSocket = new ServerSocket(port);
    
    
    /*<<< INFO >>>*/ logInfo("##### created serverSocket: "+ serverSocket.toString())
    
    
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
    serverSocket.close();
    serverSocket = null;
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    var socket : Socket = serverSocket.accept();
    
   /*<<< INFO >>>*/ logInfo("##### accepted socket: "+ socket.toString())
    
    
    var userInput: String = null
    
    try {
     // Until stopped or connection broken continue reading
     val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))
     
      /*<<< INFO >>>*/ logInfo("##### opened new bufferReader: "+ reader.toString())
    
     
     userInput = reader.readLine()
     while(!isStopped && userInput != null) {
       
       
       if( userInput != null){
           
         store(userInput)
         /*<<< INFO >>>*/ logInfo("##### userInput: "+ userInput.toString())
       
       }
       
       userInput = reader.readLine()
     }
     reader.close()
      
     /*<<< INFO >>>*/ logInfo("##### reader " + reader.toString() + "chiuso")
    

    } catch {
     case e: java.net.ConnectException =>
       // restart if could not connect to server
       restart("Error connecting to port " + port, e)
     case t: Throwable =>
       // restart if there is any other error
       restart("Error receiving data", t)
    }
  }
}