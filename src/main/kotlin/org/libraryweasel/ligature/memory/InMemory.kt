/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.memory

import io.vavr.collection.HashSet
import io.vavr.collection.Set
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import org.libraryweasel.ligature.*
import java.lang.RuntimeException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

private data class CollectionValue(val statements: Set<Statement>,
                                   val counter: AtomicLong)

class InMemoryStore: LigatureStore {
    private val collections = ConcurrentHashMap<CollectionName, CollectionValue>()
    private val lock = ReentrantReadWriteLock()
    private val open = AtomicBoolean(true)

    override suspend fun close() {
        open.set(false)
        collections.clear()
    }

    override suspend fun readTx(): ReadTx {
        if (open.get()) {
            return InMemoryReadTx(collections, lock)
        } else {
            throw RuntimeException("Store is closed.")
        }
    }

    override suspend fun writeTx(): WriteTx {
        if (open.get()) {
            return InMemoryWriteTx(collections, lock)
        } else {
            throw RuntimeException("Store is closed.")
        }
    }

    override suspend fun isOpen(): Boolean = open.get()
}

private class InMemoryReadTx(private val collections: ConcurrentHashMap<CollectionName, CollectionValue>,
                             private val lock: ReentrantReadWriteLock): ReadTx {
    private val readLock = lock.readLock()
    private val active = AtomicBoolean(true)

    init {
        readLock.lock()
    }

    override fun allStatements(collection: CollectionName): Flow<Statement> {
        TODO("Not yet implemented")
    }

    override fun cancel() {
        if (active.get()) {
            readLock.unlock()
            active.set(false)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun collections(): Flow<CollectionName> = collections.keys.asFlow()

    override fun collections(prefix: CollectionName): Flow<CollectionName> = collectionsImpl(prefix, collections)

    override fun collections(from: CollectionName, to: CollectionName): Flow<CollectionName> {
        TODO("Not yet implemented")
    }

    override fun isOpen(): Boolean = active.get()

    override fun matchStatements(collection: CollectionName, subject: Entity?, predicate: Predicate?, `object`: Object?, context: Entity?): Flow<Statement> {
        TODO("Not yet implemented")
    }

    override fun matchStatements(collection: CollectionName, subject: Entity?, predicate: Predicate?, range: Range<*>, context: Entity?): Flow<Statement> {
        TODO("Not yet implemented")
    }
}

private class InMemoryWriteTx(private val collections: ConcurrentHashMap<CollectionName, CollectionValue>,
                              private val lock: ReentrantReadWriteLock): WriteTx {
    private val writeLock = lock.writeLock()
    private val active = AtomicBoolean(true)
    private val workingState = ConcurrentHashMap(collections)

    init {
        writeLock.lock()
    }

    override fun addStatement(collection: CollectionName, statement: Statement) {
        if (active.get()) {
            createCollection(collection)
            workingState[collection] = CollectionValue(workingState[collection]!!.statements.add(statement), workingState[collection]!!.counter)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun cancel() {
        if (active.get()) {
            writeLock.unlock()
            active.set(false)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun commit() {
        if (active.get()) {
            collections.clear()
            collections.putAll(workingState)
            writeLock.unlock()
            active.set(false)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun createCollection(collection: CollectionName) {
        if (active.get()) {
            workingState.putIfAbsent(collection, CollectionValue(HashSet.empty(), AtomicLong(0)))
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun deleteCollection(collection: CollectionName) {
        if (active.get()) {
            workingState.remove(collection)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun isOpen(): Boolean = active.get()

    override fun newEntity(collection: CollectionName): Entity {
        TODO("Not yet implemented")
    }

    override fun removeStatement(collection: CollectionName, statement: Statement) {
        TODO("Not yet implemented")
    }
}

private class InMemoryCollectionReadTx(val collectionName: CollectionName,
                             private val collection: CollectionValue,
                             private val active: AtomicBoolean) {
    fun allStatements(): Flow<Statement> {
        return if (active.get()) {
            collection.statements.asFlow()
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    fun matchStatements(subject: Entity?, predicate: Predicate?, `object`: Object?, context: Entity?): Flow<Statement> {
        return if (active.get()) {
            matchStatementsImpl(collection.statements, subject, predicate, `object`, context)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    fun matchStatements(subject: Entity?, predicate: Predicate?, range: Range<*>, context: Entity?): Flow<Statement> {
        return if (active.get()) {
            matchStatementsImpl(collection.statements, subject, predicate, range, context)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }
}

private class InMemoryCollectionWriteTx(val collectionName: CollectionName,
                              private val collection: CollectionValue,
                              private val active: AtomicBoolean) {
    private var workingState = collection

    @Synchronized fun newEntity(): Entity {
        if (active.get()) {
            val newId = workingState.counter.incrementAndGet()
            workingState = CollectionValue(workingState.statements, workingState.counter)
            return Entity("_:$newId")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized fun removeStatement(statement: Statement) {
        if (active.get()) {
            workingState = CollectionValue(workingState.statements.remove(statement), workingState.counter)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized fun allStatements(): Flow<Statement> {
        return if (active.get()) {
            workingState.statements.asFlow()
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized fun matchStatements(subject: Entity?, predicate: Predicate?, `object`: Object?, context: Entity?): Flow<Statement> {
        return if (active.get()) {
            matchStatementsImpl(workingState.statements, subject, predicate, `object`, context)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized fun matchStatements(subject: Entity?, predicate: Predicate?, range: Range<*>, context: Entity?): Flow<Statement> {
        return if (active.get()) {
            matchStatementsImpl(workingState.statements, subject, predicate, range, context)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }
}

private fun collectionsImpl(prefix: CollectionName, collections: ConcurrentHashMap<CollectionName, CollectionValue>): Flow<CollectionName> {
    return collections.keys.asFlow().filter {
        it != null && it.name.startsWith(prefix.name)
    }
}

private fun matchStatementsImpl(statements: Set<Statement>, subject: Entity?, predicate: Predicate?, `object`: Object?, context: Entity?): Flow<Statement> {
    return statements.asFlow().filter {
        when (subject) {
            null -> true
            else -> (subject == it.subject)
        }
    }.filter {
        when (predicate) {
            null -> true
            else -> (predicate == it.predicate)
        }
    }.filter {
        when (`object`) {
            null -> true
            else -> (`object` == it.`object`)
        }
    }.filter {
        when (context) {
            null -> true
            else -> (context == it.context)
        }
    }
}

private fun matchStatementsImpl(statements: Set<Statement>, subject: Entity?, predicate: Predicate?, range: Range<*>, context: Entity?): Flow<Statement> {
    return statements.asFlow().filter {
        when (subject) {
            null -> true
            else -> (subject == it.subject)
        }
    }.filter {
        when (predicate) {
            null -> true
            else -> (predicate == it.predicate)
        }
    }.filter {
        when (range) {
            is LangLiteralRange -> (it.`object` is LangLiteral && ((it.`object` as LangLiteral).langTag == range.start.langTag && range.start.langTag == range.end.langTag) && (it.`object` as LangLiteral).value >= range.start.value && (it.`object` as LangLiteral).value <= range.end.value)
            is StringLiteralRange -> (it.`object` is StringLiteral && (it.`object` as StringLiteral).value >= range.start && (it.`object` as StringLiteral).value <= range.end)
            is LongLiteralRange -> (it.`object` is LongLiteral && (it.`object` as LongLiteral).value >= range.start && (it.`object` as LongLiteral).value <= range.end)
            is DoubleLiteralRange -> (it.`object` is DoubleLiteral && (it.`object` as DoubleLiteral).value >= range.start && (it.`object` as DoubleLiteral).value <= range.end)
        }
    }.filter {
        when (context) {
            null -> true
            else -> (context == it.context)
        }
    }
}
