package horizon

import org.apache.log4j.{Level, Logger}


//|||||||||||||||||||||||||||||||||||||||||||||||    MAIN    |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
/**
 *  Main object for the SPARK Twitter Analyzer app
 */  
object Main{




  /*.................................................................................................................*/
  /**
    * main method definition
    * @param args "batch" or "streaming + #batch-seconds" string to select desired execution mode
    */
  def main(args: Array[String])  {


    //app logger globall settings
    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
		Logger.getLogger("horizon").setLevel(Level.INFO)
    Logger.getLogger("it.reti.spark.sentiment").setLevel(Level.INFO)
    Logger.getLogger("it.reti.spark.sentiment.TweetApp").setLevel(Level.INFO)



    /*
    INPUT parameter check:
    according to input parameter string a different TweetApp extension class is instantiated.
    */
     val app = args(0) match {


       //BATCH case
        /*
        case "batch" =>
            val fileName = "/root/RawTweets.json"
            new TweetBatchApp(fileName)
				*/


        //STREAMING case
        case "streaming" =>
            //println("\nSelect country to spy: \n \t\t 1: Australia \n \t\t 2: London \n \t\t 3: USA \n \t\t 4: Lombardia \n \t\t 5: United Kingdom \n ")
            val location = "3" // Console.readLine()
            new TweetStreamingJSONApp(location, args(1).toInt)



        //Otherwise
        case default =>
            println("Wrong input parameter: write 'batch' or 'streaming  #batch-seconds'")
            null


    }//end match
    






    
    //run selected App
    if (app != null ) {

				Title.printTitle
				app.acquireData
		}
    
    
  }//end main method //



  
  
}//end main object |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||