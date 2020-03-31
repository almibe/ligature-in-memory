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

    override fun close() = collections.clear()

    override fun readTx(): ReadTx {
        return InMemoryReadTx(collections, lock)
    }

    override fun writeTx(): WriteTx {
        return InMemoryWriteTx(collections, lock)
    }
}

private class InMemoryReadTx(private val collections: ConcurrentHashMap<CollectionName, CollectionValue>,
                             private val lock: ReentrantReadWriteLock): ReadTx {
    private val readLock = lock.readLock()
    private val active = AtomicBoolean(true)

    init {
        readLock.lock()
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

    override fun collection(collectionName: CollectionName): CollectionReadTx? {
        val collection = collections.get(collectionName)
        return if (collection == null) {
            null
        } else {
            InMemoryCollectionReadTx(collectionName, collection, active)
        }
    }
}

private class InMemoryWriteTx(private val collections: ConcurrentHashMap<CollectionName, CollectionValue>,
                              private val lock: ReentrantReadWriteLock): WriteTx {
    private val writeLock = lock.writeLock()
    private val active = AtomicBoolean(true)

    init {
        writeLock.lock()
    }

    override fun cancel() {
        if (active.get()) {
            writeLock.unlock()
            active.set(false)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun collection(collectionName: CollectionName): CollectionWriteTx {
        val collection = collections.putIfAbsent(collectionName, CollectionValue(HashSet.empty(), AtomicLong(0)))
        return InMemoryCollectionWriteTx(collectionName, collections[collectionName]!!, active)
    }

    override fun collections(): Flow<CollectionName>  = collections.keys.asFlow()

    override fun collections(prefix: CollectionName): Flow<CollectionName> = collectionsImpl(prefix, collections)

    override fun collections(from: CollectionName, to: CollectionName): Flow<CollectionName> {
        TODO("Not yet implemented")
    }

    override fun commit() {
        TODO("Not yet implemented")
    }

//    @Synchronized override fun commit() {
//        if (active.get()) {
//            collections[name] = workingState
//            active.set(false)
//            writeLock.unlock()
//        } else {
//            throw RuntimeException("Transaction is closed.")
//        }
//    }

    override fun deleteCollection(collectionName: CollectionName) {
        collections.remove(collectionName)
    }
}

private class InMemoryCollectionReadTx(override val collectionName: CollectionName,
                             private val collection: CollectionValue,
                             private val active: AtomicBoolean): CollectionReadTx {
    override fun allStatements(): Flow<Statement> {
        return if (active.get()) {
            collection.statements.asFlow()
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun matchStatements(subject: Entity?, predicate: Predicate?, `object`: Object?, context: Entity?): Flow<Statement> {
        return if (active.get()) {
            matchStatementsImpl(collection.statements, subject, predicate, `object`, context)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun matchStatements(subject: Entity?, predicate: Predicate?, range: Range<*>, context: Entity?): Flow<Statement> {
        return if (active.get()) {
            matchStatementsImpl(collection.statements, subject, predicate, range, context)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }
}

private class InMemoryCollectionWriteTx(override val collectionName: CollectionName,
                              private val collection: CollectionValue,
                              private val active: AtomicBoolean): CollectionWriteTx {
    private var workingState = collection

    @Synchronized override fun newEntity(): Entity {
        if (active.get()) {
            val newId = workingState.counter.incrementAndGet()
            workingState = CollectionValue(workingState.statements, workingState.counter)
            return Entity("_:$newId")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun addStatement(statement: Statement) {
        if (active.get()) {
            workingState = CollectionValue(workingState.statements.add(statement), workingState.counter)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun removeStatement(statement: Statement) {
        if (active.get()) {
            workingState = CollectionValue(workingState.statements.remove(statement), workingState.counter)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun allStatements(): Flow<Statement> {
        return if (active.get()) {
            workingState.statements.asFlow()
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun matchStatements(subject: Entity?, predicate: Predicate?, `object`: Object?, context: Entity?): Flow<Statement> {
        return if (active.get()) {
            matchStatementsImpl(workingState.statements, subject, predicate, `object`, context)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun matchStatements(subject: Entity?, predicate: Predicate?, range: Range<*>, context: Entity?): Flow<Statement> {
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
