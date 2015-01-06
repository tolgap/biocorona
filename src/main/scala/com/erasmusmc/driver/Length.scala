package com.erasmusmc.driver

import com.erasmusmc.Sequence
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{Statistics, MultivariateStatisticalSummary}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object Length extends App {
  private final val SEPARATOR = ">"

  //    Set up Spark configuration and context
  val conf = new SparkConf().setAppName("SparkKmerFrequency")
  val sc = new SparkContext(conf)
  //    Create a Hadoop Job in order to change the text input delimiter
  val job = new Job(sc.hadoopConfiguration)
  job.getConfiguration.set("textinputformat.record.delimiter", SEPARATOR)

  val filePath = args(0)
  val outputPath = args(1)

  //    Using the Hadoop Job, read the file with the new delimiter
  val input = sc.newAPIHadoopFile(
    path   = filePath,
    fClass = classOf[TextInputFormat],
    kClass = classOf[LongWritable],
    vClass = classOf[Text],
    conf   = job.getConfiguration
  )

  //    Drop the line numbers and only keep the sequences
  val sequences: RDD[String] = input.map {case (_, text) => text.toString}
  //    Split the header and sequence
  val keyValue: RDD[Sequence] = sequences.flatMap(Sequence.fromString).cache

  //    Get the lengths of all sequences
  val lengths: RDD[Vector] = keyValue.map(e => Vectors.dense(e.seq.length)).cache
  lengths.saveAsTextFile(outputPath)
}
