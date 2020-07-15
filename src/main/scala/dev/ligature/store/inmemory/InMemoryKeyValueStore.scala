/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.inmemory

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.locks.ReentrantReadWriteLock

import cats.effect.{IO, Resource}
import dev.ligature.{ReadTx, WriteTx}
import dev.ligature.store.keyvalue.KeyValueStore

import scala.collection.SortedMap
import scala.util.Try

private class InMemoryKeyValueStore extends KeyValueStore {
  private val store = new AtomicReference(SortedMap[Array[Byte], Array[Byte]]())
  private val lock = new ReentrantReadWriteLock()
  private val open = new AtomicBoolean(true)

  override def compute: Resource[IO, ReadTx] = {
    Resource.make(
      IO {
        new InMemoryReadTx(store.get(), lock.readLock())
      }
    )( tx =>
      IO {
        tx.cancel()
      }
    )
  }

  override def write: Resource[IO, WriteTx] = {
    Resource.make(
      IO {
        new InMemoryWriteTx(store, lock.writeLock())
      }
    )( tx =>
      IO {
        tx.commit()
      }
    )
  }

  override def put(key: Array[Byte], value: Array[Byte]): Try[(Array[Byte], Array[Byte])] = ???

  override def delete(key: Array[Byte]): Try[Array[Byte]] = ???

  override def scan(start: Array[Byte], end: Array[Byte]): Iterable[(Array[Byte], Array[Byte])] = ???

  override def close(): Unit = {
    open.set(false)
    store.set(SortedMap[Array[Byte], Array[Byte]]())
  }
}
