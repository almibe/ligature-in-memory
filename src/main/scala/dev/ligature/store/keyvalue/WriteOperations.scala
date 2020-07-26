/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import dev.ligature.{AnonymousEntity, BooleanLiteral, DoubleLiteral, Entity, LangLiteral, LongLiteral, NamedEntity, Object, PersistedStatement, Predicate, Statement, StringLiteral}
import dev.ligature.store.keyvalue.ReadOperations.fetchCollectionId
import scodec.bits.ByteVector
import scodec.codecs.{byte, long}

import scala.util.{Success, Try}

object WriteOperations {
  def createCollection(store: KeyValueStore, collection: NamedEntity): Try[Long] = {
    val id = fetchCollectionId(store, collection)
    if (id.isEmpty) {
      val nextId = nextCollectionNameId(store)
      val collectionNameToIdKey = Encoder.encodeCollectionNameToIdKey(collection)
      val idToCollectionNameKey = Encoder.encodeIdToCollectionNameKey(nextId)
      val collectionNameToIdValue = Encoder.encodeCollectionNameToIdValue(nextId)
      val idToCollectionNameValue = Encoder.encodeIdToCollectionNameValue(collection)
      store.put(collectionNameToIdKey, collectionNameToIdValue)
      store.put(idToCollectionNameKey, idToCollectionNameValue)
      Success(nextId)
    } else {
      Success(id.get)
    }
  }

  def deleteCollection(store: KeyValueStore, collection: NamedEntity): Try[NamedEntity] = {
    val id = fetchCollectionId(store, collection)
    if (id.nonEmpty) {
      val collectionNameToIdKey = Encoder.encodeCollectionNameToIdKey(collection)
      val idToCollectionNameKey = Encoder.encodeIdToCollectionNameKey(id.get)
      store.delete(collectionNameToIdKey)
      store.delete(idToCollectionNameKey)
      Range(Prefixes.SPOC, Prefixes.IdToString + 1).foreach { prefix =>
        store.delete((byte ~~ long(64)).encode((prefix.toByte, id.get)).require.bytes)
      }
      Success(collection)
    } else {
      Success(collection)
    }
  }

  def nextCollectionNameId(store: KeyValueStore): Long = {
    val currentId = store.get(Encoder.encodeCollectionNameCounterKey())
    val nextId = currentId match {
      case Some(bv) => bv.toLong() + 1
      case None => 0
    }
    store.put(Encoder.encodeCollectionNameCounterKey(), Encoder.encodeCollectionNameCounterValue(nextId))
    nextId
  }

  def fetchOrCreateCollection(store: KeyValueStore, collectionName: NamedEntity): Long = {
    val id = fetchCollectionId(store, collectionName)
    if (id.isEmpty) {
      createCollection(store, collectionName).get
    } else {
      id.get
    }
  }

  private def nextCollectionId(store: KeyValueStore, collectionId: Long): Long = {
    val key = Encoder.encodeCollectionCounterKey(collectionId)
    val collectionCounter = store.get(key)
    val counterValue = if (collectionCounter.nonEmpty) {
      collectionCounter.get.toLong() + 1L
    } else {
      0
    }
    store.put(key, Encoder.encodeCollectionCounterValue(counterValue))
    counterValue
  }

  /**
   * Creates an new AnonymousEntity for the given collection name.
   * Returns the new AnonymousEntity.
   */
  def newEntity(store: KeyValueStore, collectionName: NamedEntity): AnonymousEntity = {
    val id = fetchOrCreateCollection(store, collectionName)
    newEntity(store, id)
  }

  private def newEntity(store: KeyValueStore, collectionId: Long): AnonymousEntity = {
    val nextId = nextCollectionId(store, collectionId)
    store.put(Encoder.encodeAnonymousEntityKey(collectionId, nextId), Encoder.empty)
    AnonymousEntity(nextId)
  }

  def subjectType(entity: Entity): Byte =
    entity match {
      case _: NamedEntity => TypeCodes.NamedEntity
      case _: AnonymousEntity => TypeCodes.AnonymousEntity
    }

  def objectType(`object`: Object): Byte =
    `object` match {
      case _: NamedEntity => TypeCodes.NamedEntity
      case _: AnonymousEntity => TypeCodes.AnonymousEntity
      case _: LangLiteral => TypeCodes.LangLiteral
      case _: StringLiteral => TypeCodes.String
      case _: BooleanLiteral => TypeCodes.Boolean
      case _: LongLiteral => TypeCodes.Long
      case _: DoubleLiteral => TypeCodes.Double
    }

  def addStatement(store: KeyValueStore,
                   collectionName: NamedEntity,
                   statement: Statement): Try[PersistedStatement] = {
    val statementResult = ReadOperations.matchStatementsImpl(store, collectionName, Some(statement.subject),
      Some(statement.predicate), Some(statement.`object`))
    if (statementResult.isEmpty) {
      val optionId = fetchCollectionId(store, collectionName)
      val collectionId = if (optionId.isEmpty) {
        createCollection(store, collectionName).get
      } else {
        optionId.get
      }
      val context = newEntity(store, collectionId)
      val subject = fetchOrCreateSubject(store, collectionId, statement.subject)
      val predicate = fetchOrCreatePredicate(store, collectionId, statement.predicate)
      val obj = fetchOrCreateObject(store, collectionId, statement.`object`)
      val subjectEncoding = Encoder.ObjectEncoding(subjectType(subject._1), subject._2)
      val objectEncoding = Encoder.ObjectEncoding(objectType(obj._1), obj._2)
      store.put(Encoder.encodeSPOC(collectionId, subjectEncoding, predicate, objectEncoding, context), ByteVector.empty)
      store.put(Encoder.encodeSOPC(collectionId, subjectEncoding, predicate, objectEncoding, context), ByteVector.empty)
      store.put(Encoder.encodePSOC(collectionId, subjectEncoding, predicate, objectEncoding, context), ByteVector.empty)
      store.put(Encoder.encodePOSC(collectionId, subjectEncoding, predicate, objectEncoding, context), ByteVector.empty)
      store.put(Encoder.encodeOSPC(collectionId, subjectEncoding, predicate, objectEncoding, context), ByteVector.empty)
      store.put(Encoder.encodeOPSC(collectionId, subjectEncoding, predicate, objectEncoding, context), ByteVector.empty)
      store.put(Encoder.encodeCSPO(collectionId, subjectEncoding, predicate, objectEncoding, context), ByteVector.empty)
      ???
    } else {
      //TODO maybe make sure only a single statement is returned
      Success(statementResult.head)
    }
  }

  private def fetchOrCreateSubject(store: KeyValueStore, collectionId: Long, subject: Entity): (Entity, Long) = {
    subject match {
      case a: AnonymousEntity => fetchOrCreateAnonymousEntity(store, collectionId, a)
      case n: NamedEntity => fetchOrCreateNamedEntity(store, collectionId, n)
    }
  }

  private def fetchOrCreatePredicate(store: KeyValueStore, collectionId: Long, predicate: Predicate): (Predicate, Long) = {
    val res = fetchPredicateId(store, collectionId, predicate)
    if (res.isEmpty) {
      createPredicate(store, collectionId, predicate)
    } else {
      (predicate, res.get)
    }
  }

  private def fetchPredicateId(store: KeyValueStore, collectionId: Long, predicate: Predicate): Option[Long] = {
    val res = store.get(Encoder.encodePredicatesToIdKey(collectionId, predicate))
    if (res.nonEmpty) {
      Some(res.get.toLong())
    } else {
      None
    }
  }

  private def createPredicate(store: KeyValueStore, collectionId: Long, predicate: Predicate): (Predicate, Long)  = {
    val nextId = nextCollectionId(store, collectionId)
    val predicatesToIdKey = Encoder.encodePredicatesToIdKey(collectionId, predicate)
    val predicatesToIdValue = Encoder.encodePredicatesToIdValue(nextId)
    val idToPredicatesKey = Encoder.encodeIdToPredicatesKey(collectionId, nextId)
    val idToPredicatesValue = Encoder.encodeIdToPredicatesValue(predicate)
    store.put(predicatesToIdKey, predicatesToIdValue)
    store.put(idToPredicatesKey, idToPredicatesValue)
    (predicate, nextId)
  }

  private def fetchOrCreateObject(store: KeyValueStore, collectionId: Long, value: Object): (Object, Long) = {
    value match {
      case a: AnonymousEntity => fetchOrCreateAnonymousEntity(store, collectionId, a)
      case n: NamedEntity => fetchOrCreateNamedEntity(store, collectionId, n)
      case l: LangLiteral => fetchOrCreateLangLiteral(store, collectionId, l)
      case d: DoubleLiteral => fetchOrCreateDoubleLiteral(store, collectionId, d)
      case l: LongLiteral => fetchOrCreateLongLiteral(store, collectionId, l)
      case s: StringLiteral => fetchOrCreateStringLiteral(store, collectionId, s)
      case b: BooleanLiteral => fetchOrCreateBooleanLiteral(store, collectionId, b)
    }
  }

  private def fetchOrCreateAnonymousEntity(store: KeyValueStore, collectionId: Long, entity: AnonymousEntity): (AnonymousEntity, Long) = {
    val res = fetchAnonymousEntityId(store, collectionId, entity)
    if (res.isEmpty) {
      createAnonymousEntity(store, collectionId, entity)
    } else {
      (entity, res.get)
    }
  }

  private def fetchAnonymousEntityId(store: KeyValueStore, collectionId: Long, entity: AnonymousEntity): Option[Long] = {
    //TODO look up in AnonymousEntities
    //TODO return accordingly
    ???
  }

  private def createAnonymousEntity(store: KeyValueStore, collectionId: Long, entity: AnonymousEntity): (AnonymousEntity, Long) = {
    //TODO get next id
    //TODO write to AnonymousEntities
    ???
  }

  private def fetchOrCreateNamedEntity(store: KeyValueStore, collectionId: Long, entity: NamedEntity): (NamedEntity, Long) = {
    val res = fetchNamedEntityId(store, collectionId, entity)
    if (res.isEmpty) {
      createNamedEntity(store, collectionId, entity)
    } else {
      (entity, res.get)
    }
  }

  private def fetchNamedEntityId(store: KeyValueStore, collectionId: Long, entity: NamedEntity): Option[Long] = {
    val res = store.get(Encoder.encodeNamedEntitiesToIdKey(collectionId, entity))
    if (res.nonEmpty) {
      Some(res.get.toLong())
    } else {
      None
    }
  }

  private def createNamedEntity(store: KeyValueStore, collectionId: Long, entity: NamedEntity): (NamedEntity, Long) = {
    val nextId = nextCollectionId(store, collectionId)
    val namedEntitiesToIdKey = Encoder.encodeNamedEntitiesToIdKey(collectionId, entity)
    val namedEntitiesToIdValue = Encoder.encodeNamedEntitiesToIdValue(nextId)
    val idToNamedEntitiesKey = Encoder.encodeIdToNamedEntitiesKey(collectionId, nextId)
    val idToNamedEntitiesValue = Encoder.encodeIdToNamedEntitiesValue(entity)
    store.put(namedEntitiesToIdKey, namedEntitiesToIdValue)
    store.put(idToNamedEntitiesKey, idToNamedEntitiesValue)
    (entity, nextId)
  }

  private def fetchOrCreateLangLiteral(store: KeyValueStore, collectionId: Long, literal: LangLiteral): (Object, Long) = {
    val res = fetchLangLiteralId(store, collectionId, literal)
    if (res.isEmpty) {
      createLangLiteral(store, collectionId, literal)
    } else {
      (literal, res.get)
    }
  }

  private def fetchLangLiteralId(store: KeyValueStore, collectionId: Long, langLiteral: LangLiteral): Option[Long] = {
    //TODO look up in LangLiteralToId
    //TODO return accordingly
    ???
  }

  private def createLangLiteral(store: KeyValueStore, collectionId: Long, langLiteral: LangLiteral): (Object, Long) = {
    //TODO get next id
    //TODO write to LangLiteralToId
    //TODO write to IdToLangLiteral
    ???
  }

  private def fetchOrCreateDoubleLiteral(store: KeyValueStore, collectionId: Long, literal: DoubleLiteral): (Object, Long) = {
    //TODO not sure I need this since I'm storing doubles directly?
    ???
  }

  private def fetchOrCreateLongLiteral(store: KeyValueStore, collectionId: Long, literal: LongLiteral): (Object, Long) = {
    //TODO not sure I need this since I'm storing longs directly?
    ???
  }

  private def fetchOrCreateStringLiteral(store: KeyValueStore, collectionId: Long, literal: StringLiteral): (Object, Long) = {
    val res = fetchStringLiteralId(store, collectionId, literal)
    if (res.isEmpty) {
      createStringLiteral(store, collectionId, literal)
    } else {
      (literal, res.get)
    }
  }

  private def fetchStringLiteralId(store: KeyValueStore, collectionId: Long, stringLiteral: StringLiteral): Option[Long] = {
    //TODO look up in StringLiteralToId
    //TODO return accordingly
    ???
  }

  private def createStringLiteral(store: KeyValueStore, collectionId: Long, stringLiteral: StringLiteral): (Object, Long) = {
    //TODO get next id
    //TODO write to StringLiteralToId
    //TODO write to IdToStringLiteral
    ???
  }

  private def fetchOrCreateBooleanLiteral(store: KeyValueStore, collectionId: Long, literal: BooleanLiteral): (Object, Long) = {
    //TODO not sure I need this since I'm storing booleans directly?
    ???
  }
}
