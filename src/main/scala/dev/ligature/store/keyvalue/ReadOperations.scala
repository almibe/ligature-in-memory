/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import dev.ligature.{AnonymousEntity, BooleanLiteral, DoubleLiteral, DoubleLiteralRange,
  Entity, LangLiteral, LangLiteralRange, Literal, LongLiteral, LongLiteralRange, NamedEntity,
  Object, PersistedStatement, Predicate, Range, Statement, StringLiteral, StringLiteralRange}

import scala.None
import scala.util.{Success, Try}

object ReadOperations {
  def collections(store: KeyValueStore): Iterable[NamedEntity] = {
    val collectionNameToId = store.scan(Encoder.collectionNamesPrefixStart,
      Encoder.collectionNamesPrefixStart)
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
    ???
//    val collectionId = fetchCollectionId(store, collectionName)
//    if (collectionId.nonEmpty) {
//      val itr = store.scan(byteBytes.encode(Prefixes.SPOC, collectionId.get).require.bytes,
//        byteBytes.encode(Prefixes.SPOC, collectionId.get).require.bytes)
//      Some(itr.map { entry: (ByteVector, ByteVector) =>
//        val attempt = spoc.decode(entry._1.bits)
//        if (attempt.isSuccessful) {
//          val (_, _, sType, eSubject, ePredicate, oType, eObj, eContext) = attempt.require.value
//          val subject = handleSubjectLookup(store, collectionId.get, sType, eSubject)
//          val predicate = handlePredicateLookup(store, collectionId.get, ePredicate)
//          val obj = handleObjectLookup(store, collectionId.get, oType, eObj)
//          val context = handleContextLookup(store, collectionId.get, eContext)
//          val statement = Statement(subject, predicate, obj)
//          PersistedStatement(collectionName, statement, context)
//        } else {
//          ???
//        }
//      })
//    } else {
//      None
//    }
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
    ???
//    statements.filter { statement =>
//      statement.statement.subject match {
//        case _ if subject.isEmpty => true
//        case _ => statement.statement.subject == subject.get
//      }
//    }.filter { statement =>
//      statement.statement.predicate match {
//        case _ if predicate.isEmpty => true
//        case _ => statement.statement.predicate == predicate.get
//      }
//    }.filter { statement =>
//      statement.statement.`object` match {
//        case _ if `object`.isEmpty => true
//        case _ => statement.statement.`object` == `object`.get
//      }
//    }
  }

  def matchStatementsImpl(store: KeyValueStore,
                          collectionName: NamedEntity,
                          subject: Option[Entity],
                          predicate: Option[Predicate],
                          range: Range[_]): Iterable[PersistedStatement] = {
    ???
//    statements.filter { statement =>
//      statement.statement.subject match {
//        case _ if subject.isEmpty => true
//        case _ => statement.statement.subject == subject.get
//      }
//    }.filter { statement =>
//      statement.statement.predicate match {
//        case _ if predicate.isEmpty => true
//        case _ => statement.statement.predicate == predicate.get
//      }
//    }.filter { statement =>
//      val s = statement.statement
//      (range, s.`object`) match {
//        case (r: LangLiteralRange, o: LangLiteral) => matchLangLiteralRange(r, o)
//        case (r: StringLiteralRange, o: StringLiteral) => matchStringLiteralRange(r, o)
//        case (r: LongLiteralRange, o: LongLiteral) => matchLongLiteralRange(r, o)
//        case (r: DoubleLiteralRange, o: DoubleLiteral) => matchDoubleLiteralRange(r, o)
//        case _ => false
//      }
//    }
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
