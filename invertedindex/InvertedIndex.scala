
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._
import java.io.File
import scala.reflect.io.Directory

object Driver {
    def main(args: Array[String]) {
      val outputaddr = "gs://invertindex-data/output/"

      val debug = args.contains("-d")
      var author: String = "*"

      if(args.contains("-a")){
        author = args(args.indexOf("-a")+1)
      }

      val bucket = "gs://invertindex-data/data/" + author + "/*"

      val conf = new SparkConf().setAppName("InvertedIndex").set("spark.hadoop.validateOutputSpecs", "False")
      conf.setMaster("local[2]")
      val sc = new SparkContext(conf)

      // Creates tuple for (word, filepath) from all files in the data folder
      // \W+ regex means split words of length 1 or more (same as [a-zA-Z0-9_]+)
      val filepaths = sc.wholeTextFiles(bucket).flatMap { case (filename, text) => text.split("""\W+""") map { word => (word, filename)}}
      
      // For local testing
      //val filepaths = sc.wholeTextFiles("./data/*/*").flatMap { case (filename, text) => text.split("""\W+""") map { word => (word, filename)}}

      // First, transform (word, file) to ((word, file, folder), count), where count starts at 1 when we find a word.
      // NOTE: docIDs will be assigned by the driver
      val counts = filepaths.map { case (word, file) => ((word, file.split("/")(file.split("/").length-1), file.split("/")(file.split("/").length-2)), 1) }
      
      // This is the reduce step. Groups all of the words and sums the counts, still separated by file name.
      .reduceByKey { case (count1, count2) => count1 + count2 }

      // This turns ((word, file, folder, id), count) into (word, (file, folder, id, count))
      .map { case ((word, file, folder), count) => (word, (file, folder, count)) }

      // This groups together the data by word.
      .groupBy { case (word, (file, folder, count)) => word }
      
      // This part is a little confusing. It basically helps format a comma separated list
      // We want to map (word, (file, count)) to (word, seq) where seq == a comma separated string of (file, count).
      .map { case (word, seq) => val seq2 = seq map { case (_, (file, folder, count)) => (file, folder, count) } 
      (word, seq2.mkString(", ")) }

      // Save it to the bucket!
      //DELETE IF EXISTS
      val directory = new Directory(new File(outputaddr)).deleteRecursively()
      counts.saveAsTextFile(outputaddr)

      // For local testing
      //val directory = new Directory(new File("./output/")).deleteRecursively()
      //counts.saveAsTextFile("./output")

      //Printing for sanity check
      if(debug){
        val output = counts.collect
        output.foreach(println)
      }

      println("Search or TopN?: ")
      var input = scala.io.StdIn.readLine()

      // implement search
      if(input.toLowerCase == "search"){
        println("What word to search for?: ")
        input = scala.io.StdIn.readLine()
        println("Finding all occurrences of " + input + "...");
        counts.collect
        val searchRes = counts.lookup(input) 
        println("\n\n\n")
        print(input)
        print(": ")
        searchRes.foreach(println)
        println("\n\n\n")
      }

      else if(input.toLowerCase == "topn"){
        println("How many results?: ")
        input = scala.io.StdIn.readLine()
        def seqOp = (accumulator: Int, element: Int) => accumulator + element
        def combOp = (accumulator1: Int, accumulator2: Int) =>  accumulator1 + accumulator2
 
        val agg = filepaths.map { case (word, file) => (word, 1) }.aggregateByKey(0)(seqOp, combOp)
        val topN = agg.top(input.toInt)(Ordering[Int].on(_._2))
        println("\n\n\n")
        topN.foreach(println)
        println("\n\n\n")
      }
  }   
} 

