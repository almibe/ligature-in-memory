/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.inmemory

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.locks.ReentrantReadWriteLock

import cats.effect.{IO, Resource}
import dev.ligature.{ReadTx, WriteTx}
import dev.ligature.store.keyvalue.KeyValueStore
import scodec.bits.ByteVector

import scala.collection.immutable.TreeMap
import scala.util.Try

private object ByteVectorOrdering extends Ordering[ByteVector] {
  def compare(a:ByteVector, b:ByteVector) = b.length compare a.length
}

private final class InMemoryKeyValueStore extends KeyValueStore {
  private val store = new AtomicReference(TreeMap[ByteVector, ByteVector]()(ByteVectorOrdering))
  private val lock = new ReentrantReadWriteLock()
  private val open = new AtomicBoolean(true)

  override def compute: Resource[IO, ReadTx] = {
    Resource.make(
      IO {
        lock.readLock().lock()
        new InMemoryReadTx(this)
      }
    )( tx =>
      IO {
        lock.readLock().unlock()
        tx.cancel()
      }
    )
  }

  override def write: Resource[IO, WriteTx] = {
    Resource.make(
      IO {
        new InMemoryWriteTx(this)
      }
    )( tx =>
      IO {
        tx.commit()
      }
    )
  }

  override def put(key: ByteVector, value: ByteVector): Try[(ByteVector, ByteVector)] = ???

  override def delete(key: ByteVector): Try[ByteVector] = ???

  override def scan(start: ByteVector, end: ByteVector): Iterable[(ByteVector, ByteVector)] = ???

  override def close(): Unit = {
    open.set(false)
    store.set(null)
  }

  override def isOpen: Boolean = ???
}
