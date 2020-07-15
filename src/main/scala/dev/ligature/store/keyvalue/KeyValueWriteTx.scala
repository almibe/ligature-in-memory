/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import cats.effect.IO
import dev.ligature.{AnonymousEntity, Entity, NamedEntity, PersistedStatement, Predicate, Statement, WriteTx}

import scala.util.Try

final class KeyValueWriteTx(private val store: KeyValueStore) extends WriteTx {
  override def createCollection(collection: NamedEntity): IO[Try[NamedEntity]] = ???

  override def deleteCollection(collection: NamedEntity): IO[Try[NamedEntity]] = ???

  override def newEntity(collection: NamedEntity): IO[Try[AnonymousEntity]] = ???

  override def addStatement(collection: NamedEntity, statement: Statement): IO[Try[PersistedStatement]] = ???

  override def removeStatement(collection: NamedEntity, statement: Statement): IO[Try[Statement]] = ???

  override def removeEntity(collection: NamedEntity, entity: Entity): IO[Try[Entity]] = ???

  override def removePredicate(collection: NamedEntity, predicate: Predicate): IO[Try[Predicate]] = ???

  override def commit(): Try[Unit] = ???

  override def cancel(): Unit = ???

  override def isOpen: Boolean = ???
}
