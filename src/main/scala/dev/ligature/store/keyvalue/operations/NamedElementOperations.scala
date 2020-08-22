/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.operations

import dev.ligature.{AnonymousElement, NamedElement}
import dev.ligature.store.keyvalue.KeyValueStore
import dev.ligature.store.keyvalue.codec.Encoder
import .removePersistedStatement

import scala.util.{Success, Try}

object NamedElementOperations {
  def handleNamedElementLookup(store: KeyValueStore, collectionId: Long, entityId: Long): NamedElement = {
    val res = store.get(Encoder.encodeIdToNamedEntitiesKey(collectionId, entityId))
    if (res.nonEmpty) {
      NamedElement(Decoder.decodeStringLiteral(res.get))
    } else {
      throw new RuntimeException(s"Not valid NamedElement - $collectionId $entityId")
    }
  }

  def fetchNamedElementId(store: KeyValueStore, collectionId: Long, entity: NamedElement): Option[Long] = {
    val res = store.get(Encoder.encodeNamedEntitiesToIdKey(collectionId, entity))
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
    val id = fetchOrCreateCollection(store, collectionName)
    newEntity(store, id)
  }

  private def newEntity(store: KeyValueStore, collectionId: Long): AnonymousElement = {
    val nextId = nextCollectionId(store, collectionId)
    store.put(Encoder.encodeAnonymousElementKey(collectionId, nextId), Encoder.empty)
    AnonymousElement(nextId)
  }


  def removeEntity(store: KeyValueStore, collectionName: NamedElement, entity: Entity): Try[Entity] = {
    //TODO check collection exists to short circuit
    val subjectMatches = ReadOperations.matchStatementsImpl(store, collectionName, Some(entity))
    subjectMatches.foreach { s =>
      removePersistedStatement(store, s)
    }
    val objectMatches = ReadOperations.matchStatementsImpl(store, collectionName, None, None, Some(entity))
    objectMatches.foreach { s =>
      removePersistedStatement(store, s)
    }
    val contextMatch = entity match {
      case c: Context => ReadOperations.statementByContextImpl(store, collectionName, c)
      case _ => None
    }
    contextMatch.foreach { s =>
      removePersistedStatement(store, s)
    }
    Success(entity)
  }

  private def fetchOrCreateNamedElement(store: KeyValueStore, collectionId: Long, entity: NamedElement): (NamedElement, Long) = {
    val res = ReadOperations.fetchNamedElementId(store, collectionId, entity)
    if (res.isEmpty) {
      createNamedElement(store, collectionId, entity)
    } else {
      (entity, res.get)
    }
  }

  private def createNamedElement(store: KeyValueStore, collectionId: Long, entity: NamedElement): (NamedElement, Long) = {
    val nextId = nextCollectionId(store, collectionId)
    val namedEntitiesToIdKey = Encoder.encodeNamedEntitiesToIdKey(collectionId, entity)
    val namedEntitiesToIdValue = Encoder.encodeNamedEntitiesToIdValue(nextId)
    val idToNamedEntitiesKey = Encoder.encodeIdToNamedEntitiesKey(collectionId, nextId)
    val idToNamedEntitiesValue = Encoder.encodeIdToNamedEntitiesValue(entity)
    store.put(namedEntitiesToIdKey, namedEntitiesToIdValue)
    store.put(idToNamedEntitiesKey, idToNamedEntitiesValue)
    (entity, nextId)
  }


}
