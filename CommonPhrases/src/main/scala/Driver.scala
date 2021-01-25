// Hadoop imports
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object Driver {
  var numWordsPerPhrase = 1
  var minInstancesNeeded = 2

  def main(args: Array[String]): Unit = {

    // Set parameters
    numWordsPerPhrase = args(1).toInt
    minInstancesNeeded = args(2).toInt

    // Set configuration
    val conf = new Configuration
    conf.set("textinputformat.record.delimiter", "\n\n")

    // Initialize job
    val job = Job.getInstance(conf, "Instances of Vote Word by Date")
    job.setJarByClass(this.getClass)

    // Set MapReduce values
    job.setMapperClass(classOf[PhrasesMapper])
    job.setCombinerClass(classOf[PhrasesReducer])
    job.setReducerClass(classOf[PhrasesReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Set IO values
    FileInputFormat.setInputPaths(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(0) + "-out"))

    // Run job
    job.waitForCompletion(true)
  }
}
