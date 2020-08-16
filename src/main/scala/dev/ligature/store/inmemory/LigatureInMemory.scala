/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.inmemory

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock

import dev.ligature._
import scodec.bits.ByteVector
import zio.{IO, Managed, UIO}

final class LigatureInMemory extends Ligature {
  override def start(): Managed[Throwable, LigatureSession] = Managed.make(
    IO {
      new LigatureInMemorySession()
    }
  )( session =>
    UIO {
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

  override def compute: Managed[Throwable, ReadTx] = {
    Managed.make(
      IO {
        lock.readLock().lock()
        new InMemoryReadTx(data)
      }
    )( _ =>
      UIO {
        lock.readLock().unlock()
      }
    )
  }

  override def write: Managed[Throwable, WriteTx] = {
    Managed.make(
      IO {
        lock.writeLock().lock()
        new InMemoryWriteTx(data)
      }
    )( tx =>
      UIO {
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
