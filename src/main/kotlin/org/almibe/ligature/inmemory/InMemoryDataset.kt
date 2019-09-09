/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.inmemory

import org.almibe.ligature.*
import java.util.*
import java.util.stream.Stream

internal class InMemoryDataset(private val name: String): Dataset {
    private data class Quad(val first: Int, val second: Int, val third: Int, val fourth: Int): Comparable<Quad> {
        override fun compareTo(other: Quad): Int {
            if (first.compareTo(other.first) != 0) {
                return first.compareTo(other.first)
            }
            if (second.compareTo(other.second) != 0) {
                return second.compareTo(other.second)
            }
            if (third.compareTo(other.third) != 0) {
                return third.compareTo(other.third)
            }
            return fourth.compareTo(fourth)
        }
    }

    private val id = Int.MIN_VALUE                        // #cnt
    private val nodeId = mutableMapOf<Node, Int>()        // #nid
    private val idNode = mutableMapOf<Int, Node>()        // #idn
    private val literalId = mutableMapOf<Literal, Int>()  // #lid
    private val idLiteral = mutableMapOf<Int, Literal>()  // #idl
    private val eavc = TreeSet<Quad>()                    // #eavc
    private val evac = TreeSet<Quad>()                    // #evac
    private val avec = TreeSet<Quad>()                    // #avec
    private val aevc = TreeSet<Quad>()                    // #aevc
    private val veac = TreeSet<Quad>()                    // #veac
    private val vaec = TreeSet<Quad>()                    // #vaec
    private val ceav = TreeSet<Quad>()                    // #ceav

    @Synchronized override fun getDatasetName(): String {
        return name
    }

    @Synchronized override fun addStatements(statements: Stream<Statement>) {
        statements.forEach {
            addStatement(it)
        }
    }

    private fun addStatement(statement: Statement) {
        TODO()
    }

    @Synchronized override fun allLiterals(): Stream<Literal> {
        return literalId.keys.stream()
    }

    @Synchronized override fun allNodes(): Stream<Node> {
        return nodeId.keys.stream()
    }

    @Synchronized override fun allStatements(): Stream<Statement> {
        return eavc.map {
            TODO()
        }.stream()
    }

    @Synchronized override fun deleteNode(node: Node) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun addNodeTypes(node: Node, types: Collection<String>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun allAttributes(): Stream<Attribute> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun allNodes(types: Collection<String>): Stream<Node> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun allTypes(): Stream<String> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun matchAll(entity: Node?, attribute: Attribute?, value: Value?, context: Node?): Stream<Statement> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun newNode(types: Collection<String>): Node {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun nodeTypes(node: Node): Collection<String> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun removeNodeTypes(node: Node, types: Collection<String>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun relabelNode(node: Node, label: String?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun removeStatements(statements: Stream<Statement>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
