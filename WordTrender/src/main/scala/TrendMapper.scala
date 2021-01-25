
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import scala.util.control.Breaks.{break, breakable}
import scala.collection.mutable.ArrayBuffer

class TrendMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  val one = new IntWritable(1)
  val day = new Text()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context) {
    val date = getDate(value.toString) // this will get the day

    val words = getWords(value.toString)

    breakable {
      words.foreach { word =>
        if (word == Driver.word) {
          day.set(date);

          context.write(day, one)

          break
        }
      }
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
}
