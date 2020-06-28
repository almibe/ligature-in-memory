/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.memory

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantReadWriteLock

import cats.effect.Resource
import dev.ligature._
import monix.eval.Task
import monix.reactive.Observable

private case class CollectionValue(statements: util.Set[PersistedStatement],
                                   counter: AtomicLong)

class InMemoryStore extends LigatureStore {
  private val collections = new ConcurrentHashMap[NamedEntity, CollectionValue]()
  private val lock = new ReentrantReadWriteLock()
  private val open = new AtomicBoolean(true)

  override def close() {
    open.set(false)
    collections.clear()
  }

  override def readTx(): Resource[Task, ReadTx] = {
    if (open.get()) {
      Resource.make(
        Task {
          new InMemoryReadTx(collections, lock.readLock())
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
    ???
//    if (open.get()) {
//      return InMemoryWriteTx(collections, lock)
//    } else {
//      throw RuntimeException("Store is closed.")
//    }
  }

  override def isOpen(): Boolean = open.get()
}

private class InMemoryReadTx(private val store: ConcurrentHashMap[NamedEntity, CollectionValue],
    private val lock: ReentrantReadWriteLock.ReadLock) extends ReadTx {
  private val active = new AtomicBoolean(true)

  lock.lock()

  override def allStatements(collection: NamedEntity): Observable[PersistedStatement] = {
    if (active.get()) {
      if (store.contains(collection)) {
        val result = store.get(collection).statements
        Observable.from(result)
      } else {
        Observable.empty
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

  override def collections(): Observable[NamedEntity] =
    Observable.from(store.keys)

  override def collections(prefix: NamedEntity): Observable[NamedEntity] =
    collectionsImpl(collections, prefix)

  override def collections(from: NamedEntity, to: NamedEntity): Observable[NamedEntity] =
    collectionsImpl(collections, from, to)

  private def collectionsImpl(collections: ConcurrentHashMap<NamedEntity, CollectionValue>, prefix: NamedEntity): Observable[NamedEntity> {
    return collections.keys.asFlow().filter {
      it != null && it.identifier.startsWith(prefix.identifier)
    }
  }

  private def collectionsImpl(collections: ConcurrentHashMap<NamedEntity, CollectionValue>, from: NamedEntity, to: NamedEntity): Observable[NamedEntity> {
    return collections.keys.asFlow().filter {
      it != null && it.identifier >= from.identifier && it.identifier < to.identifier
    }
  }

  override def isOpen(): Boolean = active.get()

  override def matchStatements(collection: NamedEntity, subject: Entity?, predicate: Predicate?, `object`: Object?): Observable[PersistedStatement> {
    return if (active.get()) {
      if (collections.containsKey(collection)) {
        matchStatementsImpl(collections[collection]!!.statements, subject, predicate, `object`)
      } else {
        setOf<PersistedStatement>().asFlow()
      }
    } else {
      throw RuntimeException("Transaction is closed.")
    }
  }

  override def matchStatements(collection: NamedEntity, subject: Entity?, predicate: Predicate?, range: Range<*>): Observable[PersistedStatement> {
    return if (active.get()) {
      if (collections.containsKey(collection)) {
        matchStatementsImpl(collections[collection]!!.statements, subject, predicate, range)
      } else {
        setOf<PersistedStatement>().asFlow()
      }
    } else {
      throw RuntimeException("Transaction is closed.")
    }
  }
}

//  private class InMemoryWriteTx(private val collections: ConcurrentHashMap<NamedEntity, CollectionValue>,
//  private val lock: ReentrantReadWriteLock): WriteTx {
//  private val writeLock = lock.writeLock()
//  private val active = AtomicBoolean(true)
//  private val workingState = ConcurrentHashMap(collections)
//
//  init {
//  writeLock.lock()
//}
//
//  @Synchronized override def addStatement(collection: NamedEntity, statement: Statement): PersistedStatement {
//  if (active.get()) {
//  createCollection(collection)
//  val context = newEntity(collection)
//  val persistedStatement = PersistedStatement(collection, statement, context)
//  workingState[collection] = CollectionValue(workingState[collection]!!.statements.add(persistedStatement), workingState[collection]!!.counter)
//  return persistedStatement
//} else {
//  throw RuntimeException("Transaction is closed.")
//}
//}
//
//  @Synchronized override def cancel() {
//  if (active.get()) {
//  writeLock.unlock()
//  active.set(false)
//} else {
//  throw RuntimeException("Transaction is closed.")
//}
//}
//
//  @Synchronized override def commit() {
//  if (active.get()) {
//  collections.clear()
//  collections.putAll(workingState)
//  writeLock.unlock()
//  active.set(false)
//} else {
//  throw RuntimeException("Transaction is closed.")
//}
//}
//
//  @Synchronized override def createCollection(collection: NamedEntity) {
//  if (active.get()) {
//  workingState.putIfAbsent(collection, CollectionValue(HashSet.empty(), AtomicLong(0)))
//} else {
//  throw RuntimeException("Transaction is closed.")
//}
//}
//
//  @Synchronized override def deleteCollection(collection: NamedEntity) {
//  if (active.get()) {
//  workingState.remove(collection)
//} else {
//  throw RuntimeException("Transaction is closed.")
//}
//}
//
//  @Synchronized override def isOpen(): Boolean = active.get()
//
//  @Synchronized override def newEntity(collection: NamedEntity): AnonymousEntity {
//  if (active.get()) {
//  createCollection(collection)
//  val newId = workingState[collection]!!.counter.incrementAndGet()
//  workingState[collection] = CollectionValue(workingState[collection]!!.statements, workingState[collection]!!.counter)
//  return AnonymousEntity(newId)
//} else {
//  throw RuntimeException("Transaction is closed.")
//}
//}
//
//  override def removeEntity(collection: NamedEntity, entity: Entity) {
//  TODO("Not yet implemented")
//}
//
//  override def removePredicate(collection: NamedEntity, predicate: Predicate) {
//  TODO("Not yet implemented")
//}
//
//  @Synchronized override def removeStatement(collection: NamedEntity, statement: Statement) {
//  if (active.get()) {
//  if (workingState.containsKey(collection)) {
//  val persistedStatement = matchStatementsImpl(workingState[collection]!!.statements,
//  statement.subject,
//  statement.predicate,
//  statement.`object`).toList()
//  if (persistedStatement.size == 1) {
//  workingState[collection] = CollectionValue(workingState[collection]!!.statements.remove(persistedStatement.first()), workingState[collection]!!.counter)
//}
//}
//} else {
//  throw RuntimeException("Transaction is closed.")
//}
//}
//}
//
//
//  private def matchStatementsImpl(statements: Set<PersistedStatement>, subject: Entity?, predicate: Predicate?, `object`: Object?): Observable[PersistedStatement> {
//  return statements.asFlow().filter {
//  when (subject) {
//  null -> true
//  else -> (subject == it.statement.subject)
//}
//}.filter {
//  when (predicate) {
//  null -> true
//  else -> (predicate == it.statement.predicate)
//}
//}.filter {
//  when (`object`) {
//  null -> true
//  else -> (`object` == it.statement.`object`)
//}
//}
//}
//
//  private def matchStatementsImpl(statements: Set<PersistedStatement>, subject: Entity?, predicate: Predicate?, range: Range<*>): Observable[PersistedStatement> {
//  return statements.asFlow().filter {
//  when (subject) {
//  null -> true
//  else -> (subject == it.statement.subject)
//}
//}.filter {
//  when (predicate) {
//  null -> true
//  else -> (predicate == it.statement.predicate)
//}
//}.filter {
//  when (range) {
//  is LangLiteralRange -> (it.statement.`object` is LangLiteral && ((it.statement.`object` as LangLiteral).langTag == range.start.langTag && range.start.langTag == range.end.langTag) && (it.statement.`object` as LangLiteral).value >= range.start.value && (it.statement.`object` as LangLiteral).value < range.end.value)
//  is StringLiteralRange -> (it.statement.`object` is StringLiteral && (it.statement.`object` as StringLiteral).value >= range.start && (it.statement.`object` as StringLiteral).value < range.end)
//  is LongLiteralRange -> (it.statement.`object` is LongLiteral && (it.statement.`object` as LongLiteral).value >= range.start && (it.statement.`object` as LongLiteral).value < range.end)
//  is DoubleLiteralRange -> (it.statement.`object` is DoubleLiteral && (it.statement.`object` as DoubleLiteral).value >= range.start && (it.statement.`object` as DoubleLiteral).value < range.end)
//}
//}
//}
