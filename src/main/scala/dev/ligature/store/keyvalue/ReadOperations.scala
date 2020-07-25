/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import dev.ligature.{AnonymousEntity, BooleanLiteral, DoubleLiteral, DoubleLiteralRange, Entity, LangLiteral, LangLiteralRange, Literal, LongLiteral, LongLiteralRange, NamedEntity, Object, PersistedStatement, Predicate, Range, Statement, StringLiteral, StringLiteralRange}
import scodec.bits.ByteVector

import scala.None
import scala.util.{Success, Try}

object ReadOperations {
  def collections(store: KeyValueStore): Iterable[NamedEntity] = {
    val collectionNameToId = store.scan(Encoder.collectionNamesPrefixStart,
      Encoder.collectionNamesPrefixEnd)
    collectionNameToId.map { encoded =>
      encoded._1.drop(1).decodeUtf8.map(NamedEntity).getOrElse(throw new RuntimeException("Invalid Name"))
    }
  }

  def fetchCollectionId(store: KeyValueStore, collectionName: NamedEntity): Option[Long] = {
    val encoded = Encoder.encodeCollectionNameToIdKey(collectionName)
    val res = store.get(encoded)
    res.map(_.toLong())
  }

  def readAllStatements(store: KeyValueStore, collectionName: NamedEntity): Option[Iterable[PersistedStatement]] = {
    val collectionId = fetchCollectionId(store, collectionName)
    if (collectionId.nonEmpty) {
      val itr = store.scan(Encoder.encodeSPOCScanStart(collectionId.get),
        Encoder.encodeSPOCScanEnd(collectionId.get))
      Some(itr.map { entry: (ByteVector, ByteVector) =>
        val attempt = Decoder.decodeSPOC(entry._1)
        if (attempt.isSuccess) {
          val spoc = attempt.get
          val subject = handleSubjectLookup(store, collectionId.get, spoc.subject.`type`, spoc.subject.id)
          val predicate = handlePredicateLookup(store, collectionId.get, spoc.predicateId)
          val obj = handleObjectLookup(store, collectionId.get, spoc.`object`.`type`, spoc.`object`.id)
          val context = handleContextLookup(store, collectionId.get, spoc.context)
          val statement = Statement(subject, predicate, obj)
          PersistedStatement(collectionName, statement, context)
        } else {
          ???
        }
      })
    } else {
      None
    }
  }

  def handleSubjectLookup(store: KeyValueStore, collectionId: Long, subjectType: Byte, subjectId: Long): Entity = {
    ???
  }

  def handlePredicateLookup(store: KeyValueStore, collectionId: Long, predicateId: Long): Predicate = {
    ???
  }

  def handleObjectLookup(store: KeyValueStore, collectionId: Long, objectType: Byte, objectValue: Long): Object = {
    ???
  }

  def handleContextLookup(store: KeyValueStore, collectionId: Long, context: Long): AnonymousEntity = {
    ???
  }

  def matchStatementsImpl(store: KeyValueStore,
                          collectionName: NamedEntity,
                          subject: Option[Entity] = None,
                          predicate: Option[Predicate] = None,
                          `object`: Option[Object] = None): Iterable[PersistedStatement] = {
    if (subject.nonEmpty) {
      if (predicate.nonEmpty) {
        ??? //TODO search SPO
      } else {
        ??? //TODO search SOP
      }
    } else if (predicate.nonEmpty) {
      if (`object`.nonEmpty) {
        ??? //TODO search POS
      } else {
        ??? //TODO search PSO
      }
    } else if (`object`.nonEmpty) {
      if (subject.nonEmpty) {
        ??? //TODO search OSP
      } else {
        ??? //TODO search OPS
      }
    } else {
      val res = readAllStatements(store, collectionName)
      if (res.nonEmpty) {
        res.get
      } else {
        Iterable.empty
      }
    }
  }

  def matchStatementsImpl(store: KeyValueStore,
                          collectionName: NamedEntity,
                          subject: Option[Entity],
                          predicate: Option[Predicate],
                          literalRange: Range[_]): Iterable[PersistedStatement] = {
    if (subject.nonEmpty) {
      ??? //TODO search SOP
    } else if (predicate.nonEmpty) {
      ??? //TODO search POS
    } else {
      if (subject.nonEmpty) {
        ??? //TODO search OSP
      } else {
        ??? //TODO search OPS
      }
    }
  }

  private def matchLangLiteralRange(range: LangLiteralRange, literal: LangLiteral): Boolean = {
    literal.langTag == range.start.langTag && range.start.langTag == range.end.langTag &&
      literal.value >= range.start.value && literal.value < range.end.value
  }

  private def matchStringLiteralRange(range: StringLiteralRange, literal: StringLiteral): Boolean = {
    literal.value >= range.start && literal.value < range.end
  }

  private def matchLongLiteralRange(range: LongLiteralRange, literal: LongLiteral): Boolean = {
    literal.value >= range.start && literal.value < range.end
  }

  private def matchDoubleLiteralRange(range: DoubleLiteralRange, literal: DoubleLiteral): Boolean = {
    literal.value >= range.start && literal.value < range.end
  }

  def statementByContextImpl(store: KeyValueStore,
                             collectionName: NamedEntity,
                             context: AnonymousEntity): Option[PersistedStatement] = ???
    //statements.find(_.context == context)
}
