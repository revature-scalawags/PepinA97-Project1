// Hadoop imports
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce._

// Scala imports
import scala.collection.mutable.ArrayBuffer

class PhrasesMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  val one = new IntWritable(1)
  val newKey = new Text()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context) {

    val words = getWords(value.toString)

    getPhrases(words, Driver.numWordsPerPhrase).foreach { phrase =>
      newKey.set(phrase);

      context.write(newKey, one)
    }
  }

  def getDate(originalStr: String): String = {
    return (originalStr.split("\n"))(1).substring(10, 12)
  }

  def getWords(originalStr: String) : Array[String] = {

    val lines = originalStr.split("\n")

    var wordsBuffer = ArrayBuffer[String]()

    for (line <- lines) {
      if(line.substring(0, 1) == "Q") {
        wordsBuffer ++= ((line.substring(2)).toLowerCase).split("\\s+")
      }
    }

    return wordsBuffer.toArray
  }

  def getPhrases(words: Array[String], numWordsPerPhrase: Int) : Array[String] = {
    var phrasesBuffer = ArrayBuffer[String]()

    for (i <- 0 until (words.length - numWordsPerPhrase - 1)) {
      var sb = new StringBuilder()

      for (j <- 0 until numWordsPerPhrase) {
        sb.append(words(i + j))

        if(j != numWordsPerPhrase) {
          sb.append(" ")
        }
      }

      phrasesBuffer += sb.toString()
    }

    return phrasesBuffer.toArray
  }
}
