/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.slonky

import cats.effect.{IO, Resource}
import dev.almibe.slonky.{Slonky, SlonkyInstance, SlonkyReadTx, SlonkyWriteTx}
import dev.ligature
import dev.ligature._

class LigatureKeyValue(private val slonky: Slonky) extends Ligature {
  override def instance: Resource[IO, LigatureInstance] = ???
}

class LigatureKeyValueInstance(private val slonkyInstance: SlonkyInstance) extends LigatureInstance {
  override def read: Resource[IO, LigatureReadTx] = ???

  override def write: Resource[IO, LigatureWriteTx] = ???
}

class LigatureKeyValueReadTx(private val slonkyReadTx: SlonkyReadTx) extends LigatureReadTx {
  override def collections: fs2.Stream[IO, NamedNode] = ???

  override def collections(prefix: NamedNode): fs2.Stream[IO, NamedNode] = ???

  override def collections(from: NamedNode, to: NamedNode): fs2.Stream[IO, NamedNode] = ???

  override def allStatements(collection: NamedNode): fs2.Stream[IO, PersistedStatement] = ???

  override def matchStatements(collection: NamedNode, subject: Option[Node], predicate: Option[NamedNode], `object`: Option[ligature.Object]): fs2.Stream[IO, PersistedStatement] = ???

  override def matchStatements(collection: NamedNode, subject: Option[Node], predicate: Option[NamedNode], range: ligature.Range): fs2.Stream[IO, PersistedStatement] = ???

  override def statementByContext(collection: NamedNode, context: AnonymousNode): IO[Option[PersistedStatement]] = ???
}

class LigatureKeyValueWriteTx(private val slonkyWriteTx: SlonkyWriteTx) extends LigatureWriteTx {
  override def createCollection(collection: NamedNode): IO[NamedNode] = ???

  override def deleteCollection(collection: NamedNode): IO[NamedNode] = ???

  override def newNode(collection: NamedNode): IO[AnonymousNode] = ???

  override def addStatement(collection: NamedNode, statement: Statement): IO[PersistedStatement] = ???

  override def removeStatement(collection: NamedNode, statement: Statement): IO[Statement] = ???

  override def cancel(): Unit = ???
}
