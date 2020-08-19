/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.inmemory

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.IO
import dev.ligature.store.keyvalue.{KeyValueStore, ReadOperations}
import dev.ligature.{AnonymousElement, Subject, NamedElement, PersistedStatement, ReadTx}

private final class InMemoryReadTx(private val store: KeyValueStore) extends ReadTx {
  private val active = new AtomicBoolean(true)

  override def allStatements(collectionName: NamedElement): IO[Iterator[PersistedStatement]] = {
    if (active.get()) {
      if (ReadOperations.fetchCollectionId(store, collectionName).nonEmpty) {
        val result = ReadOperations.readAllStatements(store, collectionName).get
        IO { result.iterator }
      } else {
        IO { Iterator.empty }
      }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }

  override def collections: IO[Iterator[NamedElement]] =
    if (active.get()) {
      IO { ReadOperations.collections(store).iterator }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }

  override def collections(prefix: NamedElement): IO[Iterator[NamedElement]] =
    IO {
//      val collectionNameToId = store.scan(Array(Prefixes.CollectionNameToId),
//        Array(Prefixes.CollectionNameToId + 1.toByte)) //TODO fix to handle prefix
      ???
    }

  override def collections(from: NamedElement, to: NamedElement): IO[Iterator[NamedElement]] =
    IO {
//      val collectionNameToId = store.scan(Array(Prefixes.CollectionNameToId),
//        Array(Prefixes.CollectionNameToId + 1.toByte)) //TODO fix to handle range
      ???
    }

  override def isOpen: Boolean = active.get()

  override def matchStatements(collectionName: NamedElement,
                               subject: Option[Subject] = None,
                               predicate: Option[NamedElement] = None,
                               `object`: Option[Subject] = None): IO[Iterator[PersistedStatement]] = {
    if (active.get()) {
      IO {
        ReadOperations.matchStatementsImpl(store, collectionName, subject, predicate, `object`).iterator
      }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }

//  override def matchStatements(collectionName: NamedElement,
//                               subject: Option[Subject],
//                               predicate: Option[NamedElement],
//                               range: Range[_]): IO[Iterator[PersistedStatement]] = {
//    if (active.get()) {
//      IO {
//        ReadOperations.matchStatementsImpl(store, collectionName, subject, predicate, range)
//      }
//    } else {
//      throw new RuntimeException("Transaction is closed.")
//    }
//  }

  override def statementByContext(collectionName: NamedElement, context: AnonymousElement):
  IO[Option[PersistedStatement]] = {
    if (active.get()) {
      IO {
        ReadOperations.statementByContextImpl(store, collectionName, context)
      }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }
}
