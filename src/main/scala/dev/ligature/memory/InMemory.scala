/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.memory

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.locks.ReentrantReadWriteLock

import cats.effect.IO
import dev.ligature._

import scala.collection.immutable.{HashMap, HashSet}
import scala.util.{Failure, Success, Try}

private case class CollectionValue(statements: AtomicReference[HashSet[PersistedStatement]],
                                   counter: AtomicLong)

class InMemoryStore extends LigatureStore {
  private val collections = new AtomicReference(new HashMap[NamedEntity, CollectionValue]())
  private val lock = new ReentrantReadWriteLock()
  private val open = new AtomicBoolean(true)

  override def close() {
    open.set(false)
    collections.set(new HashMap[NamedEntity, CollectionValue]())
  }

  override def compute[T](fun: (ReadTx) => T): IO[T] = {
    if (open.get()) {
      IO { fun(new InMemoryReadTx(collections.get(), lock.readLock())) }
    } else {
      IO { throw RuntimeException("Store is closed.") }
    }
  }

  override def write(fun: (WriteTx) => Unit): IO[Try[Unit]] = {
    if (open.get()) {
        IO {
          new InMemoryWriteTx(collections, lock.writeLock())
        }) { tx: WriteTx =>
          IO {
            if (tx.isOpen) {
              tx.commit()
            }
          }
      }
    } else {
      IO { Failure(new RuntimeException("Store is closed.")) }
    }
  }

  override def isOpen(): Boolean = open.get()
}

private class InMemoryReadTx(private val store: Map[NamedEntity, CollectionValue],
    private val lock: ReentrantReadWriteLock.ReadLock) extends ReadTx {
  private val active = new AtomicBoolean(true)

//  lock.lockInterruptibly()

  override def allStatements(collectionName: NamedEntity): Observable[PersistedStatement] = ??? // {
//    if (active.get()) {
//      val collection = store.get(collectionName)
//      if (collection.nonEmpty) {
//        val result = collection.get.statements
//        IO { Stream.fromIterable(result.get()) }
//      } else {
//        IO { Stream.empty }
//      }
//    } else {
//      throw new RuntimeException("Transaction is closed.")
//    }
//  }

  override def cancel() {
    if (active.get()) {
      lock.unlock()
      active.set(false)
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }

  override def collections(): Observable[NamedEntity] =
    Observable.fromIterable(store.keys)

  override def collections(prefix: NamedEntity): Observable[NamedEntity] = ???
//    IO { collectionsImpl(prefix) }

  override def collections(from: NamedEntity, to: NamedEntity): Observable[NamedEntity] = ???
//    IO { collectionsImpl(from, to) }

//  private def collectionsImpl(prefix: NamedEntity): Observable[NamedEntity] = {
//    Stream.fromIterable(store.keySet.filter { in =>
//      in != null && in.identifier.startsWith(prefix.identifier)
//    })
//  }

//  private def collectionsImpl(from: NamedEntity, to: NamedEntity): Observable[NamedEntity] = {
//    Stream.fromIterable(store.keySet.filter { in =>
//      in != null && in.identifier >= from.identifier && in.identifier < to.identifier
//    })
//  }

  override def isOpen(): Boolean = active.get()

  override def matchStatements(collectionName: NamedEntity,
                               subject: Option[Entity] = None,
                               predicate: Option[Predicate] = None,
                               `object`: Option[Object] = None): Observable[PersistedStatement] = ??? // {
//    if (active.get()) {
//      val collection = store.get(collectionName)
//      if (collection.nonEmpty) {
//        IO { Match.matchStatementsImpl(collection.get.statements.get(), subject, predicate, `object`) }
//      } else {
//        IO { Observable.empty }
//      }
//    } else {
//      throw new RuntimeException("Transaction is closed.")
//    }
//  }

  override def matchStatements(collectionName: NamedEntity,
                               subject: Option[Entity],
                               predicate: Option[Predicate],
                               range: Range[_]): Observable[PersistedStatement] = ??? // {
//    if (active.get()) {
//      val collection = store.get(collectionName)
//      if (collection.nonEmpty) {
//        IO { Match.matchStatementsImpl(collection.get.statements.get(), subject, predicate, range) }
//      } else {
//        IO { Observable.empty }
//      }
//    } else {
//      throw new RuntimeException("Transaction is closed.")
//    }
//  }

  override def statementByContext(collectionName: NamedEntity, context: AnonymousEntity): Option[PersistedStatement] = ??? // {
//    if (active.get()) {
//      val collection = store.get(collectionName)
//      if (collection.nonEmpty) {
//        IO { Match.statementByContextImpl(collection.get.statements.get(), context) }
//      } else {
//        IO { None }
//      }
//    } else {
//      throw new RuntimeException("Transaction is closed.")
//    }
//  }
}

//private class InMemoryWriteTx(private val store: AtomicAny[HashMap[NamedEntity, CollectionValue]],
//                              private val lock: ReentrantReadWriteLock.WriteLock) extends WriteTx {
//  private val active = Atomic(true)
//  private val workingState = Atomic(store.get())
//
//  lock.lock()
//
//  override def addStatement(collection: NamedEntity, statement: Statement): IO[Try[PersistedStatement]] = {
//    if (active.get()) {
//      val result = for {
//        col     <- createCollection(collection)
//        context <- newEntity(collection)
//        persistedStatement <- IO { PersistedStatement(collection, statement, context.get) }
//        statements <- IO { workingState.get()(collection).statements }
//        _ <- IO { statements.set(statements.get().incl(persistedStatement)) }
//      } yield Success(persistedStatement)
//      result
//    } else {
//      IO { Failure(new RuntimeException("Transaction is closed.")) }
//    }
//  }
//
//  override def cancel() {
//    if (active.get()) {
//      active.set(false)
//      lock.unlock()
//    } else {
//      throw new RuntimeException("Transaction is closed.")
//    }
//  }
//
//  override def commit(): Try[Unit] = {
//    if (active.get()) {
//      store.set(workingState.get())
//      lock.unlock()
//      active.set(false)
//      Success(())
//    } else {
//      Failure(new RuntimeException("Transaction is closed."))
//    }
//  }
//
//  override def createCollection(collection: NamedEntity): IO[Try[NamedEntity]] = {
//    if (active.get()) {
//      if (!workingState.get().contains(collection)) {
//        val oldState = workingState.get()
//        val newState = oldState.updated(collection,
//          CollectionValue(Atomic(new HashSet[PersistedStatement]()),
//            AtomicLong(0)))
//        val result = workingState.compareAndSet(oldState, newState)
//        IO { if (result) Success(collection) else Failure(new RuntimeException("Couldn't persist new collection.")) }
//      } else {
//        IO { Success(collection) } //collection exists
//      }
//    } else {
//      throw new RuntimeException("Transaction is closed.")
//    }
//  }
//
//  override def deleteCollection(collection: NamedEntity): IO[Try[NamedEntity]] = {
//    if (active.get()) {
//      val oldState = workingState.get()
//      val newState = oldState.removed(collection)
//      workingState.compareAndSet(oldState, newState)
//      IO { Success(collection) }
//    } else {
//      IO { Failure(new RuntimeException("Transaction is closed.")) }
//    }
//  }
//
//  override def isOpen(): Boolean = active.get()
//
//  override def newEntity(collection: NamedEntity): IO[Try[AnonymousEntity]] = {
//    if (active.get()) {
//      for {
//        _ <- createCollection(collection)
//        newId <- IO { workingState.get()(collection).counter.incrementAndGet() }
//      } yield Success(AnonymousEntity(newId))
//    } else {
//      IO { Failure(new RuntimeException("Transaction is closed.")) }
//    }
//  }
//
//  override def removeEntity(collection: NamedEntity, entity: Entity): IO[Try[Entity]] = {
//    if (active.get()) {
//      if (workingState.get().contains(collection)) {
//        val subjectMatches = Match.matchStatementsImpl(workingState.get()(collection).statements.get(),
//          Some(entity))
//        val objectMatches = Match.matchStatementsImpl(workingState.get()(collection).statements.get(),
//          None, None, Some(entity))
//        val contextMatch = entity match {
//          case e: AnonymousEntity => Match.statementByContextImpl(workingState.get()(collection).statements.get(), e)
//          case _ => None
//        }
//        IO {
//          subjectMatches.foreach { p =>
//            workingState
//              .get()(collection)
//              .statements.set(workingState
//              .get()(collection).statements
//              .get().excl(p))
//          }
//          objectMatches.foreach { p =>
//            workingState
//              .get()(collection)
//              .statements.set(workingState
//              .get()(collection).statements
//              .get().excl(p))
//          }
//          if (contextMatch.nonEmpty) {
//            workingState
//              .get()(collection)
//              .statements.set(workingState
//              .get()(collection).statements
//              .get().excl(contextMatch.get))
//          }
//          Success(entity)
//        }
//      } else {
//        IO { Success(entity) }
//      }
//    } else {
//      IO { Failure(new RuntimeException("Transaction is closed.")) }
//    }
//  }
//
//  override def removePredicate(collection: NamedEntity, predicate: Predicate): IO[Try[Predicate]] = {
//    if (active.get()) {
//      if (workingState.get().contains(collection)) {
//        val persistedStatement = Match.matchStatementsImpl(workingState.get()(collection).statements.get(),
//          None,
//          Some(predicate))
//        IO {
//          persistedStatement.foreach { p =>
//            workingState
//              .get()(collection)
//              .statements.set(workingState
//              .get()(collection).statements
//              .get().excl(p))
//          }
//          Success(predicate)
//        }
//      } else {
//        IO { Success(predicate) }
//      }
//    } else {
//      IO { Failure(new RuntimeException("Transaction is closed.")) }
//    }
//  }
//
//  override def removeStatement(collection: NamedEntity, statement: Statement): IO[Try[Statement]] = {
//    if (active.get()) {
//      if (workingState.get().contains(collection)) {
//        val persistedStatement = Match.matchStatementsImpl(workingState.get()(collection).statements.get(),
//          Some(statement.subject),
//          Some(statement.predicate),
//          Some(statement.`object`))
//        IO {
//          persistedStatement.foreach { p =>
//            workingState
//              .get()(collection)
//              .statements.set(workingState
//              .get()(collection).statements
//              .get().excl(p))
//          }
//          Success(statement)
//        }
//      } else {
//        IO { Success(statement) }
//      }
//    } else {
//      IO { Failure(new RuntimeException("Transaction is closed.")) }
//    }
//  }
//}
//
//private object Match {
//  def matchStatementsImpl(statements: Set[PersistedStatement],
//                          subject: Option[Entity] = None,
//                          predicate: Option[Predicate] = None,
//                          `object`: Option[Object] = None): Observable[PersistedStatement] = {
//    Stream.fromIterable(statements.filter { statement =>
//      statement.statement.subject match {
//        case _ if subject.isEmpty => true
//        case _ => statement.statement.subject == subject.get
//      }
//    }.filter { statement =>
//      statement.statement.predicate match {
//        case _ if predicate.isEmpty => true
//        case _ => statement.statement.predicate == predicate.get
//      }
//    }.filter { statement =>
//      statement.statement.`object` match {
//        case _ if `object`.isEmpty => true
//        case _ => statement.statement.`object` == `object`.get
//      }
//    })
//  }
//
//  def matchStatementsImpl(statements: Set[PersistedStatement],
//                          subject: Option[Entity],
//                          predicate: Option[Predicate],
//                          range: Range[_]): Observable[PersistedStatement] = {
//    Stream.fromIterable(statements.filter { statement =>
//      statement.statement.subject match {
//        case _ if subject.isEmpty => true
//        case _ => statement.statement.subject == subject.get
//      }
//    }.filter { statement =>
//      statement.statement.predicate match {
//        case _ if predicate.isEmpty => true
//        case _ => statement.statement.predicate == predicate.get
//      }
//    }.filter { statement =>
//      val s = statement.statement
//      (range, s.`object`) match {
//        case (r: LangLiteralRange, o: LangLiteral) => matchLangLiteralRange(r, o)
//        case (r: StringLiteralRange, o: StringLiteral) => matchStringLiteralRange(r, o)
//        case (r: LongLiteralRange, o: LongLiteral) => matchLongLiteralRange(r, o)
//        case (r: DoubleLiteralRange, o: DoubleLiteral) => matchDoubleLiteralRange(r, o)
//        case _ => false
//      }
//    })
//  }
//
//  private def matchLangLiteralRange(range: LangLiteralRange, literal: LangLiteral): Boolean = {
//    literal.langTag == range.start.langTag && range.start.langTag == range.end.langTag &&
//      literal.value >= range.start.value && literal.value < range.end.value
//  }
//
//  private def matchStringLiteralRange(range: StringLiteralRange, literal: StringLiteral): Boolean = {
//    literal.value >= range.start && literal.value < range.end
//  }
//
//  private def matchLongLiteralRange(range: LongLiteralRange, literal: LongLiteral): Boolean = {
//    literal.value >= range.start && literal.value < range.end
//  }
//
//  private def matchDoubleLiteralRange(range: DoubleLiteralRange, literal: DoubleLiteral): Boolean = {
//    literal.value >= range.start && literal.value < range.end
//  }
//
//  def statementByContextImpl(statements: Set[PersistedStatement],
//                             context: AnonymousEntity): Option[PersistedStatement] =
//    statements.find(_.context == context)
//}
