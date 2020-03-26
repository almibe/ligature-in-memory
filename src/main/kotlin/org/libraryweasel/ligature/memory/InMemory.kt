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
                                   val rules: Set<Rule>,
                                   val counter: AtomicLong)

class InMemoryStore: LigatureStore {
    private val collections = ConcurrentHashMap<Entity, CollectionValue>()
    private val lock = ReentrantReadWriteLock()

    override fun allCollections(): Flow<Entity> = collections.keys.asFlow()

    override fun close() = collections.clear()

    override fun createCollection(collectionName: Entity): LigatureCollection {
        collections.putIfAbsent(collectionName, CollectionValue(HashSet.empty(), HashSet.empty(), AtomicLong(0)))
        return InMemoryCollection(collectionName, collections, lock)
    }

    override fun collection(collectionName: Entity): LigatureCollection =
        InMemoryCollection(collectionName, collections, lock)

    override fun deleteCollection(collectionName: Entity) {
        collections.remove(collectionName)
    }
}

private class InMemoryCollection(private val name: Entity,
                                 private val collections: ConcurrentHashMap<Entity, CollectionValue>,
                                 private val lock: ReentrantReadWriteLock): LigatureCollection {
    override val collectionName: Entity
        get() = name

    override fun readTx(): ReadTx = InMemoryReadTx(name, collections, lock)

    override fun writeTx(): WriteTx = InMemoryWriteTx(name, collections, lock)
}

private class InMemoryReadTx(name: Entity,
                             collections: ConcurrentHashMap<Entity, CollectionValue>,
                             lock: ReentrantReadWriteLock): ReadTx {
    private val collection = collections[name] ?: CollectionValue(HashSet.empty(), HashSet.empty(), AtomicLong(0))
    private val active = AtomicBoolean(true)
    private val readLock = lock.readLock()

    init {
        readLock.lock()
    }

    override fun allRules(): Flow<Rule> {
        return if (active.get()) {
            collection.rules.asFlow()
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun allStatements(): Flow<Statement> {
        return if (active.get()) {
            collection.statements.asFlow()
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun cancel() {
        if (active.get()) {
            readLock.unlock()
            active.set(false)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    override fun matchRules(subject: Entity?, predicate: Predicate?, `object`: Object?): Flow<Rule> {
        return if (active.get()) {
            matchRulesImpl(collection.rules, subject, predicate, `object`)
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

private class InMemoryWriteTx(private val name: Entity,
                              private val collections: ConcurrentHashMap<Entity, CollectionValue>,
                              lock: ReentrantReadWriteLock): WriteTx {
    private val active = AtomicBoolean(true)
    private val writeLock = lock.writeLock()
    private var workingState = collections[name] ?: CollectionValue(HashSet.empty(), HashSet.empty(), AtomicLong(0))

    init {
        writeLock.lock()
    }

    @Synchronized override fun newEntity(): Entity {
        if (active.get()) {
            val newId = workingState.counter.incrementAndGet()
            workingState = CollectionValue(workingState.statements, workingState.rules, workingState.counter)
            return Entity("_:$newId")
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun addRule(rule: Rule) {
        if (active.get()) {
            workingState = CollectionValue(workingState.statements, workingState.rules.add(rule), workingState.counter)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun addStatement(statement: Statement) {
        if (active.get()) {
            workingState = CollectionValue(workingState.statements.add(statement), workingState.rules, workingState.counter)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun removeRule(rule: Rule) {
        if (active.get()) {
            workingState = CollectionValue(workingState.statements, workingState.rules.remove(rule), workingState.counter)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun removeStatement(statement: Statement) {
        if (active.get()) {
            workingState = CollectionValue(workingState.statements.remove(statement), workingState.rules, workingState.counter)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun allRules(): Flow<Rule> {
        return if (active.get()) {
            workingState.rules.asFlow()
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

    @Synchronized override fun cancel() {
        if (active.get()) {
            writeLock.unlock()
            active.set(false)
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun commit() {
        if (active.get()) {
            collections[name] = workingState
            active.set(false)
            writeLock.unlock()
        } else {
            throw RuntimeException("Transaction is closed.")
        }
    }

    @Synchronized override fun matchRules(subject: Entity?, predicate: Predicate?, `object`: Object?): Flow<Rule> {
        return if (active.get()) {
            matchRulesImpl(workingState.rules, subject, predicate, `object`)
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

private fun matchRulesImpl(rules: Set<Rule>, subject: Entity?, predicate: Predicate?, `object`: Object?): Flow<Rule> {
    return rules.asFlow().filter {
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
