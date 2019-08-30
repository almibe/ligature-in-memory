/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.inmemory

import org.almibe.ligature.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Stream

class InMemoryDataset(private val name: String): Dataset {
    data class ContextualValue(val value: Value, val context: String)
    private val id = Int.MIN_VALUE

    @Synchronized override fun getDatasetName(): String {
        return name
    }

    @Synchronized override fun addStatements(statements: Stream<Statement>) {

    }

    @Synchronized override fun allLiterals(): Stream<Literal> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun allNodes(): Stream<Node> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun allStatements(): Stream<Statement> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun deleteNode(node: Node) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun matchAll(entity: Node?, attribute: Node?, value: Value?, context: Node?): Stream<Statement> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun newNode(): Node {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun relabelNode(node: Node, label: String?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun removeStatements(statements: Stream<Statement>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
