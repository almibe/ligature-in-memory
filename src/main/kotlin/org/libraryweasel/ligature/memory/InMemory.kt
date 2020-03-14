/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.memory

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import org.libraryweasel.ligature.*
import java.util.concurrent.atomic.AtomicReference

class InMemoryStore: LigatureStore {
    private val collections: AtomicReference<io.vavr.collection.Map<Entity, InMemoryCollection>> = AtomicReference()

    override fun allCollections(): Flow<Entity> = collections.get().values().map { it.collectionName }.asFlow()

    override fun close() = collections.set(io.vavr.collection.HashMap.empty())

    override fun collection(collectionName: Entity): LigatureCollection {
        TODO("Not yet implemented")
    }

    override fun deleteCollection(collectionName: Entity) {
        TODO("Not yet implemented")
    }

    override fun details(): Map<String, String> = mapOf("location" to "memory")
}

private class InMemoryCollection(private val name: Entity): LigatureCollection {
    override val collectionName: Entity
        get() = name

    override fun readTx(): ReadTx {
        TODO("Not yet implemented")
    }

    override fun writeTx(): WriteTx {
        TODO("Not yet implemented")
    }
}

private class InMemoryReadTx: ReadTx {
    override fun allRules(): Flow<Rule> {
        TODO("Not yet implemented")
    }

    override fun allStatements(): Flow<Statement> {
        TODO("Not yet implemented")
    }

    override fun cancel() {
        TODO("Not yet implemented")
    }

    override fun matchRules(subject: Entity?, predicate: Entity?, `object`: Node?): Flow<Rule> {
        TODO("Not yet implemented")
    }

    override fun matchStatements(subject: Node?, predicate: Entity?, `object`: Node?, graph: Entity?): Flow<Statement> {
        TODO("Not yet implemented")
    }

    override fun matchStatements(subject: Node?, predicate: Entity?, range: Range<*>, graph: Entity?): Flow<Statement> {
        TODO("Not yet implemented")
    }
}

private class InMemoryWriteTx: WriteTx {
    override fun addRule(rule: Rule) {
        TODO("Not yet implemented")
    }

    override fun addStatement(statement: Statement) {
        TODO("Not yet implemented")
    }

    override fun allRules(): Flow<Rule> {
        TODO("Not yet implemented")
    }

    override fun allStatements(): Flow<Statement> {
        TODO("Not yet implemented")
    }

    override fun cancel() {
        TODO("Not yet implemented")
    }

    override fun commit() {
        TODO("Not yet implemented")
    }

    override fun matchRules(subject: Entity?, predicate: Entity?, `object`: Node?): Flow<Rule> {
        TODO("Not yet implemented")
    }

    override fun matchStatements(subject: Node?, predicate: Entity?, `object`: Node?, graph: Entity?): Flow<Statement> {
        TODO("Not yet implemented")
    }

    override fun matchStatements(subject: Node?, predicate: Entity?, range: Range<*>, graph: Entity?): Flow<Statement> {
        TODO("Not yet implemented")
    }

    override fun newEntity(): Entity {
        TODO("Not yet implemented")
    }

    override fun removeRule(rule: Rule) {
        TODO("Not yet implemented")
    }

    override fun removeStatement(statement: Statement) {
        TODO("Not yet implemented")
    }
}
