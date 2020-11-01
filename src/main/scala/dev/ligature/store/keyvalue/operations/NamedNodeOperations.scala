/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.operations

import dev.ligature.{AnonymousNode, NamedNode}
import dev.ligature.store.keyvalue.KeyValueStore
import dev.ligature.store.keyvalue.codec.{AnonymousNodeCodec, EmptyCodec, LiteralCodec, NamedNodeCodec}

object NamedNodeOperations {
  def handleNamedNodeLookup(store: KeyValueStore, collectionId: Long, entityId: Long): NamedNode = {
    val res = store.get(NamedNodeCodec.encodeIdToNamedNodesKey(collectionId, entityId))
    if (res.nonEmpty) {
      NamedNode(LiteralCodec.decodeStringLiteral(res.get))
    } else {
      throw new RuntimeException(s"Not valid NamedNode - $collectionId $entityId")
    }
  }

  def fetchNamedNodeId(store: KeyValueStore, collectionId: Long, entity: NamedNode): Option[Long] = {
    val res = store.get(NamedNodeCodec.encodeNamedNodesToIdKey(collectionId, entity))
    if (res.nonEmpty) {
      Some(res.get.toLong())
    } else {
      None
    }
  }

  /**
   * Creates an new AnonymousNode for the given collection name.
   * Returns the new AnonymousNode.
   */
  def newEntity(store: KeyValueStore, collectionName: NamedNode): AnonymousNode = {
    val id = CollectionOperations.fetchOrCreateCollection(store, collectionName)
    newEntity(store, id)
  }

  private def newEntity(store: KeyValueStore, collectionId: Long): AnonymousNode = {
    val nextId = CollectionOperations.nextCollectionId(store, collectionId)
    store.put(AnonymousNodeCodec.encodeAnonymousNodeKey(collectionId, nextId), EmptyCodec.empty)
    AnonymousNode(nextId)
  }

//  def removeEntity(store: KeyValueStore, collectionName: NamedNode, entity: Entity): Try[Entity] = {
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

  private def fetchOrCreateNamedNode(store: KeyValueStore, collectionId: Long, namedElement: NamedNode): (NamedNode, Long) = {
    val res = NamedNodeOperations.fetchNamedNodeId(store, collectionId, namedElement)
    if (res.isEmpty) {
      createNamedNode(store, collectionId, namedElement)
    } else {
      (namedElement, res.get)
    }
  }

  private def createNamedNode(store: KeyValueStore, collectionId: Long, entity: NamedNode): (NamedNode, Long) = {
    val nextId = CollectionOperations.nextCollectionId(store, collectionId)
    val namedElementsToIdKey = NamedNodeCodec.encodeNamedNodesToIdKey(collectionId, entity)
    val namedElementsToIdValue = NamedNodeCodec.encodeNamedNodesToIdValue(nextId)
    val idToNamedNodesKey = NamedNodeCodec.encodeIdToNamedNodesKey(collectionId, nextId)
    val idToNamedNodesValue = NamedNodeCodec.encodeIdToNamedNodesValue(entity)
    store.put(namedElementsToIdKey, namedElementsToIdValue)
    store.put(idToNamedNodesKey, idToNamedNodesValue)
    (entity, nextId)
  }
}
