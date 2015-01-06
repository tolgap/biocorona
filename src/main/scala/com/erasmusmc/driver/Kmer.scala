package com.erasmusmc.driver

import com.erasmusmc.Sequence

import scala.collection.mutable

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object Kmer extends App {

  private final val SEPARATOR        = ">"
  private final val NEW_LINE         = "\n"
  private final val OUTPUT_SEPERATOR = "\t"
  private final val EMPTY            = ""
  private final val NUCLEOTIDES      = List("A", "T", "C", "G")
  private final val AMBIGUOUS_NUC    = List(
    'M', 'R', 'W', 'S', 'Y', 'K', 'V', 'H',
    'D', 'B', 'X', 'N'
  )

  //    Set up Spark configuration and context
  val conf = new SparkConf().setAppName("SparkKmerFrequency")
  val sc = new SparkContext(conf)
  //    Create a Hadoop Job in order to change the text input delimiter
  val job = new Job(sc.hadoopConfiguration)
  job.getConfiguration.set("textinputformat.record.delimiter", SEPARATOR)

  val filePath = args(0)
  val slidingWindow = args(1).toInt
  val outputPath = args(2)

  //    Using the Hadoop Job, read the file with the new delimiter
  val input = sc.newAPIHadoopFile(
    path   = filePath,
    fClass = classOf[TextInputFormat],
    kClass = classOf[LongWritable],
    vClass = classOf[Text],
    conf   = job.getConfiguration
  )

  //    Drop the line numbers and only keep the sequences
  val sequences = input.map {case (_, text) => text.toString}
  //    Split the header and sequence
  //    Split the sequence to a maximum of 1000 bps.
  val keyValue: RDD[Sequence] = sequences.flatMap(Sequence.fromString).flatMap(_ split 1000).cache

  //    Create all possible combinations for the current
  //    sliding window in a MutableMap
  val combinations = mutable.Map(permutationsWithRepetition(NUCLEOTIDES, slidingWindow)
    .map(e => (e.mkString, 0D)).toMap.toSeq: _*)
  //    Calculate the kmer frequencies by mapping over the keyValue RDD
  val kmerFrequencies: RDD[String] = keyValue.map(calcFrequencies(_, combinations.clone(), slidingWindow))
  kmerFrequencies.saveAsTextFile(outputPath)

  def splitKeyValue(sequence: String): Option[Sequence] = {
    val kv = sequence.trim.split(NEW_LINE, 2)
    if (kv.size != 2) return None
    val seq = new Sequence(kv(0), kv(1).replaceAllLiterally(NEW_LINE, EMPTY))
    Some(seq)
  }

  def calcFrequencies(seq: Sequence, combinations: mutable.Map[String, Double], sw: Int): String = {
    seq.sliding(sw).filterNot(isAmbiguous).foreach {
      case kmer =>
        if (kmer.length == sw) {
          val count = combinations(kmer)
          combinations(kmer) = count + 1D
        }
    }

    val total = combinations.values.sum
    val frequencies = combinations.values.map(_ / total)
    seq.header + OUTPUT_SEPERATOR + frequencies.mkString(OUTPUT_SEPERATOR)
  }

  def isAmbiguous(kmer: String): Boolean = {
    AMBIGUOUS_NUC.filter(kmer contains _).nonEmpty
  }

  def permutationsWithRepetition[T](input: List[T], n: Int): List[List[T]] = {
    n match {
      case 1 => for (el <- input) yield List(el)
      case _ => for (el <- input; perm <- permutationsWithRepetition(input, n - 1)) yield el::perm
    }
  }

}
