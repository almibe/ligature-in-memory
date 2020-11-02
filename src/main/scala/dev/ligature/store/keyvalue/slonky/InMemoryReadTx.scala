/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.slonky

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.IO
import dev.ligature.store.keyvalue.operations.{CollectionOperations, StatementOperations}
import dev.ligature.{AnonymousNode, LigatureReadTx, NamedNode, Node, PersistedStatement}
import fs2.Stream

private final class InMemoryReadTx(private val store: KeyValueStore) extends LigatureReadTx {
  private val active = new AtomicBoolean(true)

  override def allStatements(collectionName: NamedNode): IO[Iterator[PersistedStatement]] = {
    if (active.get()) {
      if (CollectionOperations.fetchCollectionId(store, collectionName).nonEmpty) {
        val result = StatementOperations.readAllStatements(store, collectionName).get
        IO { result.iterator }
      } else {
        IO { Iterator.empty }
      }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }

  override def collections: IO[Iterator[NamedNode]] =
    if (active.get()) {
      IO { CollectionOperations.collections(store).iterator }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }

  override def collections(prefix: NamedNode): Stream[IO, NamedNode] =
    IO {
//      val collectionNameToId = store.scan(Array(Prefixes.CollectionNameToId),
//        Array(Prefixes.CollectionNameToId + 1.toByte)) //TODO fix to handle prefix
      ???
    }

  override def collections(from: NamedNode, to: NamedNode): Stream[IO, NamedNode] =
    IO {
//      val collectionNameToId = store.scan(Array(Prefixes.CollectionNameToId),
//        Array(Prefixes.CollectionNameToId + 1.toByte)) //TODO fix to handle range
      ???
    }

  override def isOpen: Boolean = active.get()

  override def matchStatements(collectionName: NamedNode,
                               subject: Option[Node] = None,
                               predicate: Option[NamedNode] = None,
                               `object`: Option[Object] = None): IO[Iterator[PersistedStatement]] = {
    if (active.get()) {
      IO {
        StatementOperations.matchStatementsImpl(store, collectionName, subject, predicate, `object`).iterator
      }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }

//  override def matchStatements(collectionName: NamedNode,
//                               subject: Option[Node],
//                               predicate: Option[NamedNode],
//                               range: Range[_]): IO[Iterator[PersistedStatement]] = {
//    if (active.get()) {
//      IO {
//        ReadOperations.matchStatementsImpl(store, collectionName, subject, predicate, range)
//      }
//    } else {
//      throw new RuntimeException("Transaction is closed.")
//    }
//  }

  override def statementByContext(collectionName: NamedNode, context: AnonymousNode):
  IO[Option[PersistedStatement]] = {
    if (active.get()) {
      IO {
        StatementOperations.statementByContextImpl(store, collectionName, context)
      }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }
}
