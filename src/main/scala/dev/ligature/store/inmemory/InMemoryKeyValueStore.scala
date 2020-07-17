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

private final class InMemoryKeyValueStore extends KeyValueStore {
  private val store = new AtomicReference(TreeMap[ByteVector, ByteVector]()(ByteVectorOrdering))
  private val lock = new ReentrantReadWriteLock()
  private val open = new AtomicBoolean(true)

  private object ByteVectorOrdering extends Ordering[ByteVector] {
    def compare(a:ByteVector, b:ByteVector): Int = b.length compare a.length
  }

  def compute: Resource[IO, ReadTx] = {
    Resource.make(
      IO {
        lock.readLock().lock()
        new InMemoryReadTx(this)
      }
    )( _ =>
      IO {
        lock.readLock().unlock()
      }
    )
  }

  def write: Resource[IO, WriteTx] = {
    Resource.make(
      IO {
        lock.writeLock().lock()
        new InMemoryWriteTx(this)
      }
    )( tx =>
      IO {
        lock.writeLock().unlock()
        tx.commit()
      }
    )
  }

  override def put(key: ByteVector, value: ByteVector): Try[(ByteVector, ByteVector)] = ???

  override def delete(key: ByteVector): Try[ByteVector] = ???

  override def scan(start: ByteVector, end: ByteVector): Iterable[(ByteVector, ByteVector)] = ???

  def close(): Unit = {
    open.set(false)
    store.set(null)
  }

  def isOpen: Boolean = open.get()
}
