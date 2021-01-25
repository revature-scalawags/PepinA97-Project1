
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce._

import scala.jdk.CollectionConverters._

class TrendReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context) {

    var sum = values.asScala.foldLeft(0)(_ + _.get)

    context.write(key, new IntWritable(sum))
  }
}

