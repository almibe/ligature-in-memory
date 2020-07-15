/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import cats.effect.IO
import dev.ligature
import dev.ligature.{AnonymousEntity, Entity, NamedEntity, PersistedStatement, Predicate, ReadTx}

final class KeyValueReadTx(private val store: KeyValueStore) extends ReadTx {
  override def collections(): IO[Iterable[NamedEntity]] = ???

  override def collections(prefix: NamedEntity): IO[Iterable[NamedEntity]] = ???

  override def collections(from: NamedEntity, to: NamedEntity): IO[Iterable[NamedEntity]] = ???

  override def allStatements(collection: NamedEntity): IO[Iterable[PersistedStatement]] = ???

  override def matchStatements(collection: NamedEntity, subject: Option[Entity], predicate: Option[Predicate], `object`: Option[ligature.Object]): IO[Iterable[PersistedStatement]] = ???

  override def matchStatements(collection: NamedEntity, subject: Option[Entity], predicate: Option[Predicate], range: ligature.Range[_]): IO[Iterable[PersistedStatement]] = ???

  override def statementByContext(collection: NamedEntity, context: AnonymousEntity): IO[Option[PersistedStatement]] = ???

  override def cancel(): Unit = ???

  override def isOpen: Boolean = ???
}
