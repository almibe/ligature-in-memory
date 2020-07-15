/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.inmemory

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock

import cats.effect.IO
import dev.ligature.store.keyvalue.{KeyValueStore, Prefixes}
import dev.ligature.{AnonymousEntity, Entity, NamedEntity, Object, PersistedStatement, Predicate, Range, ReadTx}

private class InMemoryReadTx(private val store: KeyValueStore,
                             private val lock: ReentrantReadWriteLock.ReadLock) extends ReadTx {
  private val active = new AtomicBoolean(true)
  lock.lock()

  override def allStatements(collectionName: NamedEntity): IO[Iterable[PersistedStatement]] = {
    if (active.get()) {
      if (Common.collectionExists(store, collectionName)) {
        val result = Common.readAllStatements(store, collectionName)
        IO { result }
      } else {
        IO { Iterable.empty }
      }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }

  override def cancel() {
    if (active.get()) {
      lock.unlock()
      active.set(false)
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }

  override def collections(): IO[Iterable[NamedEntity]] =
    IO {
      val collectionNameToId = store.scan(Array(Prefixes.CollectionNameToId),
        Array(Prefixes.CollectionNameToId + 1.toByte))
      ???
    }

  override def collections(prefix: NamedEntity): IO[Iterable[NamedEntity]] =
    IO {
      val collectionNameToId = store.scan(Array(Prefixes.CollectionNameToId),
        Array(Prefixes.CollectionNameToId + 1.toByte)) //TODO fix to handle prefix
      ???
    }

  override def collections(from: NamedEntity, to: NamedEntity): IO[Iterable[NamedEntity]] =
    IO {
      val collectionNameToId = store.scan(Array(Prefixes.CollectionNameToId),
        Array(Prefixes.CollectionNameToId + 1.toByte)) //TODO fix to handle range
      ???
    }

  override def isOpen(): Boolean = active.get()

  override def matchStatements(collectionName: NamedEntity,
                               subject: Option[Entity] = None,
                               predicate: Option[Predicate] = None,
                               `object`: Option[Object] = None): IO[Iterable[PersistedStatement]] = {
    if (active.get()) {
      IO {
        Common.matchStatementsImpl(store, collectionName, subject, predicate, `object`)
      }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }

  override def matchStatements(collectionName: NamedEntity,
                               subject: Option[Entity],
                               predicate: Option[Predicate],
                               range: Range[_]): IO[Iterable[PersistedStatement]] = {
    if (active.get()) {
      IO {
        Common.matchStatementsImpl(store, collectionName, subject, predicate, range)
      }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }

  override def statementByContext(collectionName: NamedEntity, context: AnonymousEntity):
  IO[Option[PersistedStatement]] = {
    if (active.get()) {
      IO {
        Common.statementByContextImpl(store, collectionName, context)
      }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }
}
