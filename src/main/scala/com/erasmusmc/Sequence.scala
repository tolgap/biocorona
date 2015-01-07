package com.erasmusmc

class Sequence(header: String, seq: String) {
  private final val UNDERSCORE    = "_"
  private final val OUTPUT_SEP    = "\t"
  private final val AMBIGUOUS_NUC = List(
    'M', 'R', 'W', 'S', 'Y', 'K', 'V', 'H',
    'D', 'B', 'X', 'N'
  )
  private final val NUCLEOTIDES    = List("A", "T", "C", "G")

  def split(length: Int): Iterator[Sequence] = {
    val seqs = seq.grouped(length)
    seqs.zipWithIndex.map {
      case(s,h) => new Sequence(header + UNDERSCORE + h, s)
    }
  }

//  Delegates to String.sliding(Int, Int)
  def slidingWindow(sw: Int): Iterator[String] = {
    seq.sliding(sw, sw).filter(_.length == sw)
  }

//  Calculate the k-mer frequency based on the
//  sliding window. It results in a String
//  with the format:
//  Header \t frequencies \n
  def kmerFrequency(sw: Int): String = {
    var combinations = Map(Sequence.permutationsWithRepetition(NUCLEOTIDES, sw)
        .map(e => (e.mkString, 0D)).toMap.toSeq: _*)
    slidingWindow(sw).filterNot(isAmbiguous).foreach {
      case kmer =>
        if (kmer.length == sw) {
          val count = combinations(kmer)
          combinations = combinations.updated(kmer, count + 1D)
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
    if (str.length < 20) return None
    val kv = str.trim.split(NEW_LINE, 2).lift
    (kv(0), kv(1)) match {
      case (Some(k), Some(s)) =>
        val seq = new Sequence(k, s.replaceAllLiterally(NEW_LINE, EMPTY))
        Some(seq)
      case _ => None
    }
  }

  def permutationsWithRepetition[T](input: List[T], n: Int): List[List[T]] = {
    n match {
      case 1 => for (el <- input) yield List(el)
      case _ => for (el <- input; perm <- permutationsWithRepetition(input, n - 1)) yield el::perm
    }
  }
}
