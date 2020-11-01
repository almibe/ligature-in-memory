/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.inmemory

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock

import cats.effect.{IO, Resource}
import dev.ligature._
import scodec.bits.ByteVector

final class LigatureInMemory extends Ligature {
  override def start(): Resource[IO, LigatureSession] = Resource.make(
    IO {
      new LigatureInMemorySession()
    }
  )( session =>
    IO {
      session.close()
    }
  )
}

final class LigatureInMemorySession extends LigatureSession {
  private val data = InMemoryKeyValueStore.newStore()
  private val lock = new ReentrantReadWriteLock()
  private val open = new AtomicBoolean(true)

  def close(): Unit = {
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

  def isOpen: Boolean = open.get()

  def debugDump(): Iterable[(ByteVector, ByteVector)] = { //TODO probably remove eventually
    data.debugDump()
  }
}
