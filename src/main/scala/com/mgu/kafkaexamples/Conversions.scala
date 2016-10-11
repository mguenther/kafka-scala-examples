package com.mgu.kafkaexamples

import java.util.function.{ Consumer => JConsumer, BiConsumer => JBiConsumer }

object Conversions {

  def cons[T](f: (T) => Unit): JConsumer[T] = new JConsumer[T] {
    override def accept(t: T) = f(t)
  }

  def cons[T,U](f: (T, U) => Unit): JBiConsumer[T, U] = new JBiConsumer[T,U] {
    override def accept(t: T, u: U) = f(t, u)
  }
}
