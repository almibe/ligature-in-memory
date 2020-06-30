/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.memory

import java.util.concurrent.locks.ReentrantReadWriteLock

import cats.effect.Resource
import dev.ligature._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.execution.atomic.{Atomic, AtomicAny, AtomicBoolean, AtomicLong}
import monix.reactive.Observable

import scala.collection.immutable.{HashMap, HashSet}
import scala.util.{Failure, Success, Try}

private case class CollectionValue(statements: AtomicAny[HashSet[PersistedStatement]],
                                   counter: AtomicLong)

class InMemoryStore extends LigatureStore {
  private val collections = Atomic(new HashMap[NamedEntity, CollectionValue]())
  private val lock = new ReentrantReadWriteLock()
  private val open = Atomic(true)

  override def close() {
    open.set(false)
    collections.set(new HashMap[NamedEntity, CollectionValue]())
  }

  override def readTx(): Resource[Task, ReadTx] = {
    if (open.get()) {
      Resource.make(
        Task {
          new InMemoryReadTx(collections.get(), lock.readLock())
        }) { in: ReadTx =>
          Task {
            if (in.isOpen)
              in.cancel()
          }
        }
    } else {
      throw new RuntimeException("Store is closed.")
    }
  }

  override def writeTx(): Resource[Task, WriteTx] = {
    if (open.get()) {
      Resource.make(
        Task {
          new InMemoryWriteTx(collections, lock.writeLock())
        }) { tx: WriteTx =>
          Task {
            if (tx.isOpen) {
              tx.commit()
            }
          }
      }
    } else {
      throw new RuntimeException("Store is closed.")
    }
  }

  override def isOpen(): Boolean = open.get()
}

private class InMemoryReadTx(private val store: Map[NamedEntity, CollectionValue],
    private val lock: ReentrantReadWriteLock.ReadLock) extends ReadTx {
  private val active = Atomic(true)

  lock.lock()

  override def allStatements(collectionName: NamedEntity): Task[Observable[PersistedStatement]] = {
    if (active.get()) {
      val collection = store.get(collectionName)
      if (collection.nonEmpty) {
        val result = collection.get.statements
        Task { Observable.from(result.get()) }
      } else {
        Task { Observable.empty }
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

  override def collections(): Task[Observable[NamedEntity]] =
    Task { Observable.from(store.keys) }

  override def collections(prefix: NamedEntity): Task[Observable[NamedEntity]] =
    Task { collectionsImpl(prefix) }

  override def collections(from: NamedEntity, to: NamedEntity): Task[Observable[NamedEntity]] =
    Task { collectionsImpl(from, to) }

  private def collectionsImpl(prefix: NamedEntity): Observable[NamedEntity] = {
    Observable.from(store.keySet.filter { in =>
      in != null && in.identifier.startsWith(prefix.identifier)
    })
  }

  private def collectionsImpl(from: NamedEntity, to: NamedEntity): Observable[NamedEntity] = {
    Observable.from(store.keySet.filter { in =>
      in != null && in.identifier >= from.identifier && in.identifier < to.identifier
    })
  }

  override def isOpen(): Boolean = active.get()

  override def matchStatements(collectionName: NamedEntity,
                               subject: Entity = null,
                               predicate: Predicate = null,
                               `object`: Object = null): Task[Observable[PersistedStatement]] = {
    if (active.get()) {
      val collection = store.get(collectionName)
      if (collection.nonEmpty) {
        Task { matchStatementsImpl(collection.get.statements.get(), subject, predicate, `object`) }
      } else {
        Task { Observable.empty }
      }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }

  override def matchStatements(collectionName: NamedEntity,
                               subject: Entity,
                               predicate: Predicate,
                               range: Range[_]): Task[Observable[PersistedStatement]] = {
    if (active.get()) {
      val collection = store.get(collectionName)
      if (collection.nonEmpty) {
        Task { matchStatementsImpl(collection.get.statements.get(), subject, predicate, range) }
      } else {
        Task { Observable.empty }
      }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }
}

private class InMemoryWriteTx(private val store: AtomicAny[HashMap[NamedEntity, CollectionValue]],
                              private val lock: ReentrantReadWriteLock.WriteLock) extends WriteTx {
  private val active = Atomic(true)
  private val workingState = Atomic(store.get())

  lock.lock()

  override def addStatement(collection: NamedEntity, statement: Statement): Task[Try[PersistedStatement]] = {
    if (active.get()) {
      val result = for {
        col     <- createCollection(collection)
        context <- newEntity(collection)
        persistedStatement <- Task { PersistedStatement(collection, statement, context.get) }
        statements <- Task { workingState.get()(collection).statements }
        _ <- Task { statements.set(statements.get().incl(persistedStatement)) }
      } yield Success(persistedStatement)
      result
    } else {
      Task { Failure(new RuntimeException("Transaction is closed.")) }
    }
  }

  override def cancel() {
    if (active.get()) {
      active.set(false)
      lock.unlock()
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }

  override def commit(): Try[Unit] = {
    if (active.get()) {
      store.set(workingState.get())
      lock.unlock()
      active.set(false)
      Success(())
    } else {
      Failure(new RuntimeException("Transaction is closed."))
    }
  }

  override def createCollection(collection: NamedEntity): Task[Try[NamedEntity]] = {
    if (active.get()) {
      if (!workingState.get().contains(collection)) {
        val oldState = workingState.get()
        val newState = oldState.updated(collection,
          CollectionValue(Atomic(new HashSet[PersistedStatement]()),
            AtomicLong(0)))
        val result = workingState.compareAndSet(oldState, newState)
        Task { if (result) Success(collection) else Failure(new RuntimeException("Couldn't persist new collection.")) }
      } else {
        Task { Success(collection) } //collection exists
      }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }

  override def deleteCollection(collection: NamedEntity): Task[Try[NamedEntity]] = {
    if (active.get()) {
      val oldState = workingState.get()
      val newState = oldState.removed(collection)
      workingState.compareAndSet(oldState, newState)
      Task { Success(collection) }
    } else {
      Task { Failure(new RuntimeException("Transaction is closed.")) }
    }
  }

  override def isOpen(): Boolean = active.get()

  override def newEntity(collection: NamedEntity): Task[Try[AnonymousEntity]] = {
    if (active.get()) {
      for {
        _ <- createCollection(collection)
        newId <- Task { workingState.get()(collection).counter.incrementAndGet() }
      } yield Success(AnonymousEntity(newId))
    } else {
      Task { Failure(new RuntimeException("Transaction is closed.")) }
    }
  }

  override def removeEntity(collection: NamedEntity, entity: Entity): Task[Try[Entity]] = {
    ???
  }

  override def removePredicate(collection: NamedEntity, predicate: Predicate): Task[Try[Predicate]] = {
    ???
  }

  override def removeStatement(collection: NamedEntity, statement: Statement): Task[Try[Statement]] = {
    if (active.get()) {
      if (workingState.get().contains(collection)) {
        for {
          persistedStatement <- Match.matchStatementsImpl(workingState.get()(collection).statements.get(),
            statement.subject,
            statement.predicate,
            statement.`object`)
          _ <- Task { workingState.get()(collection).statements.set(workingState.get()(collection).statements.get().-(persistedStatement)) }
        } yield Success(persistedStatement.statement)
      } else {
        Task { Success(statement) }
      }
    } else {
      Task { Failure(new RuntimeException("Transaction is closed.")) }
    }
  }
}

private object Match {
  def matchStatementsImpl(statements: Set[PersistedStatement],
                                  subject: Entity,
                                  predicate: Predicate,
                                  `object`: Object): Observable[PersistedStatement] = {
    ???
    //    statements.filter {
    //      match (subject) {
    //        null -> true
    //        else -> (subject == it.statement.subject)
    //      }
    //    }.filter {
    //      when (predicate) {
    //        null -> true
    //        else -> (predicate == it.statement.predicate)
    //      }
    //    }.filter {
    //      when (`object`) {
    //        null -> true
    //        else -> (`object` == it.statement.`object`)
    //      }
    //    }
  }

  def matchStatementsImpl(statements: Set[PersistedStatement],
                                  subject: Entity,
                                  predicate: Predicate,
                                  range: Range[_]): Observable[PersistedStatement] = {
    ???
    //    statements.asFlow().filter {
    //      when (subject) {
    //        null -> true
    //        else -> (subject == it.statement.subject)
    //      }
    //    }.filter {
    //      when (predicate) {
    //        null -> true
    //        else -> (predicate == it.statement.predicate)
    //      }
    //    }.filter {
    //      when (range) {
    //        is LangLiteralRange -> (it.statement.`object` is LangLiteral && ((it.statement.`object` as LangLiteral).langTag == range.start.langTag && range.start.langTag == range.end.langTag) && (it.statement.`object` as LangLiteral).value >= range.start.value && (it.statement.`object` as LangLiteral).value < range.end.value)
    //        is StringLiteralRange -> (it.statement.`object` is StringLiteral && (it.statement.`object` as StringLiteral).value >= range.start && (it.statement.`object` as StringLiteral).value < range.end)
    //        is LongLiteralRange -> (it.statement.`object` is LongLiteral && (it.statement.`object` as LongLiteral).value >= range.start && (it.statement.`object` as LongLiteral).value < range.end)
    //        is DoubleLiteralRange -> (it.statement.`object` is DoubleLiteral && (it.statement.`object` as DoubleLiteral).value >= range.start && (it.statement.`object` as DoubleLiteral).value < range.end)
    //      }
    //    }
  }
}