/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.libraryweasel.ligature.memory

import kotlinx.coroutines.flow.Flow
import org.libraryweasel.ligature.*

class InMemoryStore: LigatureStore {
    override fun allCollections(): Flow<LigatureCollection> {
        TODO("Not yet implemented")
    }

    override fun close() {
        TODO("Not yet implemented")
    }

    override fun collection(collectionName: String): LigatureCollection {
        TODO("Not yet implemented")
    }

    override fun createCollection(collectionName: String): LigatureCollection {
        TODO("Not yet implemented")
    }

    override fun deleteCollection(collectionName: String) {
        TODO("Not yet implemented")
    }

    override fun details(): Map<String, String> {
        TODO("Not yet implemented")
    }
}

private class InMemoryCollection(private val name: String): LigatureCollection {
    override val collectionName: String
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

    override fun matchRules(entity: Identifier?, attribute: Identifier?, value: Node?): Flow<Rule> {
        TODO("Not yet implemented")
    }

    override fun matchStatements(entity: Node?, attribute: Identifier?, value: Node?): Flow<Statement> {
        TODO("Not yet implemented")
    }

    override fun matchStatements(entity: Node?, attribute: Identifier?, range: Range): Flow<Statement> {
        TODO("Not yet implemented")
    }

    override fun sparqlQuery(query: String): Any? {
        TODO("Not yet implemented")
    }

    override fun wanderQuery(query: String): Any? {
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

    override fun matchRules(entity: Identifier?, attribute: Identifier?, value: Node?): Flow<Rule> {
        TODO("Not yet implemented")
    }

    override fun matchStatements(entity: Node?, attribute: Identifier?, value: Node?): Flow<Statement> {
        TODO("Not yet implemented")
    }

    override fun matchStatements(entity: Node?, attribute: Identifier?, range: Range): Flow<Statement> {
        TODO("Not yet implemented")
    }

    override fun newIdentifier(): Identifier {
        TODO("Not yet implemented")
    }

    override fun removeRule(rule: Rule) {
        TODO("Not yet implemented")
    }

    override fun removeStatement(statement: Statement) {
        TODO("Not yet implemented")
    }

    override fun sparqlQuery(query: String): Any? {
        TODO("Not yet implemented")
    }

    override fun wanderQuery(query: String): Any? {
        TODO("Not yet implemented")
    }
}
