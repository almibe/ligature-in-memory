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
    private val attributeId = mutableMapOf<Node, Int>()   // #nid
    private val idAttribute = mutableMapOf<Int, Node>()   // #idn
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

    @Synchronized override fun newNode(): Node {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun addStatements(statements: Stream<Statement>) {
        statements.forEach {
            addStatement(it)
        }
    }

    private fun addStatement(statement: Statement) {
        TODO()
    }

    @Synchronized override fun matchAll(entity: Node?, attribute: Attribute?, value: Value?, context: Node?): Stream<Statement> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Synchronized override fun removeStatements(statements: Stream<Statement>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
