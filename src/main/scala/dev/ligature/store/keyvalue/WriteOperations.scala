/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import dev.ligature.{AnonymousEntity, BooleanLiteral, DoubleLiteral, Entity, LangLiteral, LongLiteral, NamedEntity, Object, PersistedStatement, Predicate, Statement, StringLiteral}
import dev.ligature.store.keyvalue.ReadOperations.fetchCollectionId

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
        ??? //store.delete((byte ~~ bytes).encode((prefix.toByte, id)).require.bytes)
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

  /**
   * Creates an new AnonymousEntity for the given collection name.
   * Returns the new AnonymousEntity.
   */
  def newEntity(store: KeyValueStore, collectionName: NamedEntity): AnonymousEntity = {
    val id = fetchOrCreateCollection(store, collectionName)
    val key = Encoder.encodeCollectionCounterKey(id)
    val collectionCounter = store.get(key)
    val counterValue = if (collectionCounter.nonEmpty) {
      collectionCounter.get.toLong() + 1L
    } else {
      0
    }
    store.put(key, Encoder.encodeCollectionCounterValue(counterValue))
    AnonymousEntity(counterValue)
    store.put(Encoder.encodeAnonymousEntityKey(id, counterValue), Encoder.empty)
    AnonymousEntity(counterValue)
  }

  def addStatement(store: KeyValueStore,
                   collectionName: NamedEntity,
                   statement: Statement): Try[PersistedStatement] = {
    //TODO check if statement already exists
    val optionId = fetchCollectionId(store, collectionName)
    val id = if (optionId.isEmpty) {
      createCollection(store, collectionName).get
    } else {
      optionId.get
    }
    val context = newEntity(store, collectionName)
    val subject = fetchOrCreateSubject(store, collectionName, statement.subject)
    val predicate = fetchOrCreatePredicate(store, collectionName, statement.predicate)
    val obj = fetchOrCreateObject(store, collectionName, statement.`object`)
    //TODO store.put(spoc.encode((Prefixes.SPOC, id)).require.bytes, ByteVector.empty)
    //TODO store.put(sopc.encode(???).require.bytes, ByteVector.empty)
    //TODO store.put(psoc.encode(???).require.bytes, ByteVector.empty)
    //TODO store.put(posc.encode(???).require.bytes, ByteVector.empty)
    //TODO store.put(ospc.encode(???).require.bytes, ByteVector.empty)
    //TODO store.put(opsc.encode(???).require.bytes, ByteVector.empty)
    //TODO store.put(cspo.encode(???).require.bytes, ByteVector.empty)
    ???
    //      persistedStatement <- IO { PersistedStatement(collection, statement, context.get) }
    //      statements <- IO { workingState.get()(collection).statements }
    //      _ <- IO { statements.set(statements.get().incl(persistedStatement)) }
  }

  def fetchOrCreateSubject(store: KeyValueStore, collectionName: NamedEntity, subject: Entity): Long = {
    subject match {
      case a: AnonymousEntity => fetchOrCreateAnonymousEntity(store, collectionName, a)
      case n: NamedEntity => fetchOrCreateNamedEntity(store, collectionName, n)
    }
  }

  def fetchOrCreatePredicate(store: KeyValueStore, collectionName: NamedEntity, predicate: Predicate): Long = {
    ???
  }

  def fetchOrCreateObject(store: KeyValueStore, collectionName: NamedEntity, value: Object): Long = {
    value match {
      case a: AnonymousEntity => fetchOrCreateAnonymousEntity(store, collectionName, a)
      case n: NamedEntity => fetchOrCreateNamedEntity(store, collectionName, n)
      case l: LangLiteral => fetchOrCreateLangLiteral(store, collectionName, l)
      case d: DoubleLiteral => fetchOrCreateDoubleLiteral(store, collectionName, d)
      case l: LongLiteral => fetchOrCreateLongLiteral(store, collectionName, l)
      case s: StringLiteral => fetchOrCreateStringLiteral(store, collectionName, s)
      case b: BooleanLiteral => fetchOrCreateBooleanLiteral(store, collectionName, b)
    }
  }

  def fetchOrCreateAnonymousEntity(store: KeyValueStore, collectionName: NamedEntity, entity: AnonymousEntity): Long = {
    ???
  }

  def fetchOrCreateNamedEntity(store: KeyValueStore, collectionName: NamedEntity, entity: NamedEntity): Long = {
    ???
  }

  def fetchOrCreateLangLiteral(store: KeyValueStore, collectionName: NamedEntity, literal: LangLiteral): Long = {
    ???
  }

  def fetchOrCreateDoubleLiteral(store: KeyValueStore, collectionName: NamedEntity, literal: DoubleLiteral): Long = {
    ???
  }

  def fetchOrCreateLongLiteral(store: KeyValueStore, collectionName: NamedEntity, literal: LongLiteral): Long = {
    ???
  }

  def fetchOrCreateStringLiteral(store: KeyValueStore, collectionName: NamedEntity, literal: StringLiteral): Long = {
    ???
  }

  def fetchOrCreateBooleanLiteral(store: KeyValueStore, collectionName: NamedEntity, literal: BooleanLiteral): Long = {
    ???
  }
}
