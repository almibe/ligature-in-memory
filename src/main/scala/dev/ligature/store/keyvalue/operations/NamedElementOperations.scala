/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.operations

import dev.ligature.{AnonymousElement, NamedElement}
import dev.ligature.store.keyvalue.KeyValueStore
import dev.ligature.store.keyvalue.codec.{AnonymousElementCodec, EmptyCodec, LiteralCodec, NamedElementCodec}

object NamedElementOperations {
  def handleNamedElementLookup(store: KeyValueStore, collectionId: Long, entityId: Long): NamedElement = {
    val res = store.get(NamedElementCodec.encodeIdToNamedElementsKey(collectionId, entityId))
    if (res.nonEmpty) {
      NamedElement(LiteralCodec.decodeStringLiteral(res.get))
    } else {
      throw new RuntimeException(s"Not valid NamedElement - $collectionId $entityId")
    }
  }

  def fetchNamedElementId(store: KeyValueStore, collectionId: Long, entity: NamedElement): Option[Long] = {
    val res = store.get(NamedElementCodec.encodeNamedElementsToIdKey(collectionId, entity))
    if (res.nonEmpty) {
      Some(res.get.toLong())
    } else {
      None
    }
  }

  /**
   * Creates an new AnonymousElement for the given collection name.
   * Returns the new AnonymousElement.
   */
  def newEntity(store: KeyValueStore, collectionName: NamedElement): AnonymousElement = {
    val id = CollectionOperations.fetchOrCreateCollection(store, collectionName)
    newEntity(store, id)
  }

  private def newEntity(store: KeyValueStore, collectionId: Long): AnonymousElement = {
    val nextId = CollectionOperations.nextCollectionId(store, collectionId)
    store.put(AnonymousElementCodec.encodeAnonymousElementKey(collectionId, nextId), EmptyCodec.empty)
    AnonymousElement(nextId)
  }

//  def removeEntity(store: KeyValueStore, collectionName: NamedElement, entity: Entity): Try[Entity] = {
//    //TODO check collection exists to short circuit
//    val subjectMatches = ReadOperations.matchStatementsImpl(store, collectionName, Some(entity))
//    subjectMatches.foreach { s =>
//      removePersistedStatement(store, s)
//    }
//    val objectMatches = ReadOperations.matchStatementsImpl(store, collectionName, None, None, Some(entity))
//    objectMatches.foreach { s =>
//      removePersistedStatement(store, s)
//    }
//    val contextMatch = entity match {
//      case c: Context => ReadOperations.statementByContextImpl(store, collectionName, c)
//      case _ => None
//    }
//    contextMatch.foreach { s =>
//      removePersistedStatement(store, s)
//    }
//    Success(entity)
//  }

  private def fetchOrCreateNamedElement(store: KeyValueStore, collectionId: Long, namedElement: NamedElement): (NamedElement, Long) = {
    val res = NamedElementOperations.fetchNamedElementId(store, collectionId, namedElement)
    if (res.isEmpty) {
      createNamedElement(store, collectionId, namedElement)
    } else {
      (namedElement, res.get)
    }
  }

  private def createNamedElement(store: KeyValueStore, collectionId: Long, entity: NamedElement): (NamedElement, Long) = {
    val nextId = CollectionOperations.nextCollectionId(store, collectionId)
    val namedElementsToIdKey = NamedElementCodec.encodeNamedElementsToIdKey(collectionId, entity)
    val namedElementsToIdValue = NamedElementCodec.encodeNamedElementsToIdValue(nextId)
    val idToNamedElementsKey = NamedElementCodec.encodeIdToNamedElementsKey(collectionId, nextId)
    val idToNamedElementsValue = NamedElementCodec.encodeIdToNamedElementsValue(entity)
    store.put(namedElementsToIdKey, namedElementsToIdValue)
    store.put(idToNamedElementsKey, idToNamedElementsValue)
    (entity, nextId)
  }
}
