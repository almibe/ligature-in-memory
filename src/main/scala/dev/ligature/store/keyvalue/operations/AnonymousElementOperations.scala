/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.operations

import dev.ligature.AnonymousNode
import dev.ligature.store.keyvalue.KeyValueStore
import dev.ligature.store.keyvalue.codec.AnonymousNodeCodec

object AnonymousNodeOperations {
  def fetchAnonymousNodeId(store: KeyValueStore, collectionId: Long, entity: AnonymousNode): Option[Long] = {
    val res = store.get(AnonymousNodeCodec.encodeAnonymousNodeKey(collectionId, entity.identifier))
    if (res.nonEmpty) {
      Some(entity.identifier)
    } else {
      None
    }
  }

  def newAnonymousNode(store: KeyValueStore, l: Long): AnonymousNode = {
    ???
  }

  private def fetchOrCreateAnonymousNode(store: KeyValueStore, collectionId: Long, entity: AnonymousNode): (AnonymousNode, Long) = {
    val res = fetchAnonymousNodeId(store, collectionId, entity)
    if (res.isEmpty) {
      createAnonymousNode(store, collectionId, entity)
    } else {
      (entity, res.get)
    }
  }

  private def createAnonymousNode(store: KeyValueStore, collectionId: Long, entity: AnonymousNode): (AnonymousNode, Long) = {
    //TODO get next id
    //TODO write to AnonymousEntities
    ???
  }
}
