package com.mgu.kafkaexamples.util

import java.util.function.{BiConsumer => JBiConsumer, Consumer => JConsumer}

object Conversions {

  def cons[T](f: (T) => Unit): JConsumer[T] = new JConsumer[T] {
    override def accept(t: T) = f(t)
  }

  def cons[T,U](f: (T, U) => Unit): JBiConsumer[T, U] = new JBiConsumer[T,U] {
    override def accept(t: T, u: U) = f(t, u)
  }
}
