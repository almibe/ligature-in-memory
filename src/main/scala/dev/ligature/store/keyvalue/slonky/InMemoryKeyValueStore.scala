/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.slonky

import java.util.concurrent.atomic.AtomicReference

import scodec.bits.ByteVector

import scala.collection.immutable.TreeMap
import scala.util.{Success, Try}

/**
 * An implementation of KeyValueStore that uses an AtomicReference to a TreeMap to store its data.
 */
private final class InMemoryKeyValueStore(private val data: AtomicReference[TreeMap[ByteVector, ByteVector]])
  extends KeyValueStore {

  override def get(key: ByteVector): Option[ByteVector] = data.get().get(key)

  override def put(key: ByteVector, value: ByteVector): Try[(ByteVector, ByteVector)] = {
    val current = data.get()
    val newValue = current.updated(key, value)
    data.set(newValue)
    Success((key, value))
  }

  override def delete(key: ByteVector): Try[ByteVector] = {
    val current = data.get()
    val newValue = current.removed(key)
    data.set(newValue)
    Success(key)
  }

  override def prefix(prefix: ByteVector): Iterable[(ByteVector, ByteVector)] = {
    data.get().rangeFrom(prefix).takeWhile { bv => bv._1.startsWith(prefix) }
  }

  override def range(start: ByteVector, end: ByteVector): Iterable[(ByteVector, ByteVector)] =
    data.get().range(start, end)

  def copy(): InMemoryKeyValueStore = {
    val ref = new AtomicReference(this.data.get())
    new InMemoryKeyValueStore(ref)
  }

  def clear(): Unit = data.set(null)

  def commit(newValue: InMemoryKeyValueStore): Unit = {
    this.data.set(newValue.data.get())
  }

  def debugDump(): Iterable[(ByteVector, ByteVector)] = {
    data.get()
  }
}

private object InMemoryKeyValueStore {
  object ByteVectorOrdering extends Ordering[ByteVector] {
    //TODO from https://github.com/scodec/scodec-bits/pull/196
    def compare(a:ByteVector, b:ByteVector): Int = {
      if (a.eq(b)) {
        0
      } else {
        val thisLength = a.length
        val thatLength = b.length
        val commonLength = thisLength.min(thatLength)
        var i = 0
        while (i < commonLength) {
          val cmp = a(i).compare(b(i))
          if (cmp != 0) {
            return cmp
          }
          i = i + 1
        }
        if (thisLength < thatLength) {
          -1
        } else if (thisLength > thatLength) {
          1
        } else {
          0
        }
      }
    }
  }

  def newStore(): InMemoryKeyValueStore = {
    new InMemoryKeyValueStore(new AtomicReference(
      TreeMap[ByteVector, ByteVector]()(ByteVectorOrdering)))
  }
}
