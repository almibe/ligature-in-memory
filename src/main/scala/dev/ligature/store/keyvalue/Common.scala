/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import cats.effect.IO
import dev.ligature.store.inmemory.InMemoryKeyValueStore
import dev.ligature.{AnonymousEntity, DoubleLiteral, DoubleLiteralRange, Entity, LangLiteral, LangLiteralRange, LongLiteral, LongLiteralRange, NamedEntity, Object, PersistedStatement, Predicate, Range, StringLiteral, StringLiteralRange}
import scodec.bits.ByteVector
import scodec.codecs.{byte, long, utf8} //TODO get rid of this import
import dev.ligature.store.keyvalue.Encoders.{byteString, byteBytes}

import scala.util.{Success, Try}

object Common {
  def collections(store: InMemoryKeyValueStore): IO[Iterable[NamedEntity]] = {
    IO {
      val collectionNameToId = store.scan(ByteVector.fromByte(Prefixes.CollectionNameToId),
        ByteVector.fromByte((Prefixes.CollectionNameToId + 1.toByte).toByte))
      collectionNameToId.map { encoded =>
        encoded._1.drop(1).decodeUtf8.map(NamedEntity).getOrElse(throw new RuntimeException("Invalid Name"))
      }
    }
  }

  def createCollection(store: KeyValueStore, collection: NamedEntity): IO[Try[NamedEntity]] = {
    if (collectionExists(store, collection).isEmpty) {
      IO {
        val nextId = nextCollectionNameId(store)
        val collectionNameToIdEncoder = byte ~ utf8
        val idToCollectionNameEncoder = byte ~ long(64)
        val collectionNameToIdEncodedKey = collectionNameToIdEncoder.encode((Prefixes.CollectionNameToId, collection.identifier)).require.bytes
        val idToCollectionNameEncodedKey = idToCollectionNameEncoder.encode((Prefixes.IdToCollectionName, nextId)).require.bytes
        store.put(collectionNameToIdEncodedKey, long(64).encode(nextId).require.bytes)
        store.put(idToCollectionNameEncodedKey, utf8.encode(collection.identifier).require.bytes)
        Success(collection)
      }
    } else {
      IO { Success(collection) }
    }
  }

  def deleteCollection(store: KeyValueStore, collection: NamedEntity): IO[Try[NamedEntity]] = {
    IO {
      val id = collectionExists(store, collection).orNull
      if (id != null) {
        store.delete(byteString.encode(Prefixes.CollectionNameToId, collection.identifier).require.bytes)
        store.delete(byteBytes.encode(Prefixes.IdToCollectionName, id).require.bytes)
        Range(Prefixes.SPOC, Prefixes.IdToString + 1).foreach { prefix =>
          store.delete(byteBytes.encode(prefix.toByte, id).require.bytes)
        }
        Success(collection)
      } else {
        Success(collection)
      }
    }
  }

  def nextCollectionNameId(store: KeyValueStore): Long = {
    val currentId = store.get(byte.encode(Prefixes.CollectionNameCounter).require.bytes)
    currentId match {
      case Some(bv) => {
        val nextId = bv.toLong() + 1
        store.put(byte.encode(Prefixes.CollectionNameCounter).require.bytes, long(64).encode(nextId).require.bytes)
        nextId
      }
      case None => {
        val nextId = 0
        store.put(byte.encode(Prefixes.CollectionNameCounter).require.bytes, long(64).encode(nextId).require.bytes)
        nextId
      }
    }
  }

  def collectionExists(store: KeyValueStore, collectionName: NamedEntity): Option[ByteVector] = {
    val encoder = byte ~ utf8
    val encoded = encoder.encode(Prefixes.CollectionNameToId, collectionName.identifier).require.bytes
    store.get(encoded)
  }

  def readAllStatements(store: KeyValueStore, collectionName: NamedEntity): Iterable[PersistedStatement] = {
    ???
  }

  def collectionId(store: KeyValueStore, collectionName: NamedEntity): Long = {
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
