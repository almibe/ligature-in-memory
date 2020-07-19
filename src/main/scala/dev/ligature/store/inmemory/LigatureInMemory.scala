/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.inmemory

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock

import cats.effect.{IO, Resource}
import dev.ligature._

final class LigatureInMemory extends Ligature {
  private val data = InMemoryKeyValueStore.newStore()
  private val lock = new ReentrantReadWriteLock()
  private val open = new AtomicBoolean(true)

  override def close(): Unit = {
    open.set(false)
    data.clear()
  }

  override def compute: Resource[IO, ReadTx] = {
    Resource.make(
      IO {
        lock.readLock().lock()
        new InMemoryReadTx(data)
      }
    )( _ =>
      IO {
        lock.readLock().unlock()
      }
    )
  }

  override def write: Resource[IO, WriteTx] = {
    Resource.make(
      IO {
        lock.writeLock().lock()
        new InMemoryWriteTx(data)
      }
    )( tx =>
      IO {
        tx.commit()
        lock.writeLock().unlock()
      }
    )
  }

  override def isOpen(): Boolean = open.get()
}
