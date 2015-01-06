package com.erasmusmc

class Sequence(header: String, seq: String) {
  private final val UNDERSCORE = "_"

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
    if (str.lines.length < 2) return None
    val kv = str.trim.split(NEW_LINE, 2).lift
    (kv(0), kv(1)) match {
      case (Some(k), Some(s)) =>
        val seq = new Sequence(k, s.replaceAllLiterally(NEW_LINE, EMPTY))
        Some(seq)
      case _ => None
    }
  }
}
