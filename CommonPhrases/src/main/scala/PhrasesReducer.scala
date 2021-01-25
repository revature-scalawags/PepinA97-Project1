// Hadoop imports
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce._

// Scala imports
import scala.jdk.CollectionConverters.IterableHasAsScala

class PhrasesReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context) {
    var sum = values.asScala.foldLeft(0)(_ + _.get)

    if(sum > Driver.minInstancesNeeded) {
      context.write(key, new IntWritable(sum))
    }
  }
}
