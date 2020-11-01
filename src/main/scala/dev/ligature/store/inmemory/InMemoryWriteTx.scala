/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.inmemory

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.IO
import dev.ligature._
import dev.ligature.store.keyvalue.operations.{NamedNodeOperations, CollectionOperations, StatementOperations}

import scala.util.{Failure, Success, Try}

private final class InMemoryWriteTx(val store: InMemoryKeyValueStore) extends WriteTx {
  private val active = new AtomicBoolean(true)
  private val workingState = store.copy()

  override def addStatement(collection: NamedElement, statement: Statement): IO[Try[PersistedStatement]] = {
    if (active.get()) {
      IO { StatementOperations.addStatement(workingState, collection, statement) }
    } else {
      IO { Failure(new RuntimeException("Transaction is closed.")) }
    }
  }

  override def cancel() {
    active.set(false)
    workingState.clear()
  }

  def commit(): Try[Unit] = {
    if (active.get()) {
      store.commit(workingState)
      active.set(false)
      workingState.clear()
      Success(())
    } else {
      Failure(new RuntimeException("Transaction is closed."))
    }
  }

  override def createCollection(collection: NamedElement): IO[Try[NamedElement]] =
    if (active.get()) {
      IO {
        CollectionOperations.createCollection(workingState, collection)
        Success(collection)
      }
    } else {
      IO { Failure(new RuntimeException("Transaction is closed.")) }
    }

  override def deleteCollection(collection: NamedElement): IO[Try[NamedElement]] = {
    if (active.get()) {
      IO {
        CollectionOperations.deleteCollection(workingState, collection)
      }
    } else {
      IO { Failure(new RuntimeException("Transaction is closed.")) }
    }
  }

  override def isOpen: Boolean = active.get()

  override def newEntity(collection: NamedElement): IO[Try[AnonymousElement]] = {
    if (active.get()) {
      IO { Success(NamedNodeOperations.newEntity(workingState, collection)) }
    } else {
      IO { Failure(new RuntimeException("Transaction is closed.")) }
    }
  }

//  override def removeEntity(collection: NamedElement, entity: Entity): IO[Try[Entity]] = {
//    if (active.get()) {
//      IO { WriteOperations.removeEntity(workingState, collection, entity) }
//    } else {
//      IO { Failure(new RuntimeException("Transaction is closed.")) }
//    }
//  }
//
//  override def removePredicate(collection: NamedElement, predicate: Predicate): IO[Try[Predicate]] = {
//    if (active.get()) {
//      IO { WriteOperations.removePredicate(workingState, collection, predicate) }
//    } else {
//      IO { Failure(new RuntimeException("Transaction is closed.")) }
//    }
//  }
//
//  override def removeStatement(collection: NamedElement, statement: Statement): IO[Try[Statement]] = {
//    if (active.get()) {
//      IO { WriteOperations.removeStatement(workingState, collection, statement) }
//    } else {
//      IO { Failure(new RuntimeException("Transaction is closed.")) }
//    }
//  }
}
