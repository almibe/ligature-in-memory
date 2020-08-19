/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.inmemory

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.IO
import dev.ligature._
import dev.ligature.store.keyvalue.WriteOperations

import scala.util.{Failure, Success, Try}

private final class InMemoryWriteTx(val store: InMemoryKeyValueStore) extends WriteTx {
  private val active = new AtomicBoolean(true)
  private val workingState = store.copy()

  override def addStatement(collection: NamedEntity, statement: Statement): IO[Try[PersistedStatement]] = {
    if (active.get()) {
      IO { WriteOperations.addStatement(workingState, collection, statement) }
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

  override def createCollection(collection: NamedEntity): IO[Try[NamedEntity]] =
    if (active.get()) {
      IO {
        WriteOperations.createCollection(workingState, collection)
        Success(collection)
      }
    } else {
      IO { Failure(new RuntimeException("Transaction is closed.")) }
    }

  override def deleteCollection(collection: NamedEntity): IO[Try[NamedEntity]] = {
    if (active.get()) {
      IO {
        WriteOperations.deleteCollection(workingState, collection)
      }
    } else {
      IO { Failure(new RuntimeException("Transaction is closed.")) }
    }
  }

  override def isOpen: Boolean = active.get()

  override def newEntity(collection: NamedEntity): IO[Try[AnonymousEntity]] = {
    if (active.get()) {
      IO { Success(WriteOperations.newEntity(workingState, collection)) }
    } else {
      IO { Failure(new RuntimeException("Transaction is closed.")) }
    }
  }

//  override def removeEntity(collection: NamedEntity, entity: Entity): IO[Try[Entity]] = {
//    if (active.get()) {
//      IO { WriteOperations.removeEntity(workingState, collection, entity) }
//    } else {
//      IO { Failure(new RuntimeException("Transaction is closed.")) }
//    }
//  }
//
//  override def removePredicate(collection: NamedEntity, predicate: Predicate): IO[Try[Predicate]] = {
//    if (active.get()) {
//      IO { WriteOperations.removePredicate(workingState, collection, predicate) }
//    } else {
//      IO { Failure(new RuntimeException("Transaction is closed.")) }
//    }
//  }
//
//  override def removeStatement(collection: NamedEntity, statement: Statement): IO[Try[Statement]] = {
//    if (active.get()) {
//      IO { WriteOperations.removeStatement(workingState, collection, statement) }
//    } else {
//      IO { Failure(new RuntimeException("Transaction is closed.")) }
//    }
//  }
}
