package com.erasmusmc

import scala.collection.mutable

class Sequence(header: String, seq: String) {
  private final val UNDERSCORE    = "_"
  private final val OUTPUT_SEP    = "\t"
  private final val AMBIGUOUS_NUC = List(
    'M', 'R', 'W', 'S', 'Y', 'K', 'V', 'H',
    'D', 'B', 'X', 'N'
  )

  def split(length: Int): Iterator[Sequence] = {
    val seqs = seq.grouped(length)
    seqs.zipWithIndex.map {
      case(s,h) => new Sequence(header + UNDERSCORE + h, s)
    }
  }

//  Delegates to String.sliding(Int)
  def sliding(sw: Int): Iterator[String] = {
    seq.sliding(sw)
  }

//  Calculate the k-mer frequency based on the
//  sliding window. It results in a String
//  with the format:
//  Header \t frequencies \n
  def kmerFrequency(combinations: mutable.Map[String, Double], sw: Int): String = {
    sliding(sw).filterNot(isAmbiguous).foreach {
      case kmer =>
        if (kmer.length == sw) {
          val count = combinations(kmer)
          combinations(kmer) = count + 1D
        }

    }
    val total = combinations.values.sum
    val frequencies = combinations.values.map(_ / total)
    header + OUTPUT_SEP + frequencies.mkString(OUTPUT_SEP)
  }

//  Checks if a (sub)sequence contains
//  abmiguous nucleotides.
  def isAmbiguous(kmer: String): Boolean = {
    AMBIGUOUS_NUC.filter(kmer contains _).nonEmpty
  }

//  Getters for the class
  def header(): String = header
  def seq():    String = seq
}

object Sequence {
  private final val NEW_LINE = "\n"
  private final val EMPTY = ""

//  Create possible Sequence from a String.
//  Note: this uses a lifted array
//  to fully utilize the Option monad.
  def fromString(str: String): Option[Sequence] = {
    val kv = str.trim.split(NEW_LINE, 2).lift
    (kv(0), kv(1)) match {
      case (Some(k), Some(s)) =>
        val seq = new Sequence(k, s.replaceAllLiterally(NEW_LINE, EMPTY))
        Some(seq)
      case _ => None
    }
  }
}
