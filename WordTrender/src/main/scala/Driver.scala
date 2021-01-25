// Hadoop imports
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object Driver {
  var word = ""

  def main(args: Array[String]): Unit = {
    // Set parameters
    word = args(1)

    // Set configuration
    val conf = new Configuration
    conf.set("textinputformat.record.delimiter", "\n\n")

    // Initialize job
    val job = Job.getInstance(conf, "Instances of Vote Word by Date")
    job.setJarByClass(this.getClass)

    // Set MapReduce values
    job.setMapperClass(classOf[TrendMapper])
    job.setCombinerClass(classOf[TrendReducer])
    job.setReducerClass(classOf[TrendReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Set IO values
    FileInputFormat.setInputPaths(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(0) + "-out"))

    // Run job
    job.waitForCompletion(true)
  }
}
