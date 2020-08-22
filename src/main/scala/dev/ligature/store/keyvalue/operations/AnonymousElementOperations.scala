/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.operations

import dev.ligature.AnonymousElement
import dev.ligature.store.keyvalue.KeyValueStore
import dev.ligature.store.keyvalue.codec.Encoder

object AnonymousElementOperations {
  def fetchAnonymousElementId(store: KeyValueStore, collectionId: Long, entity: AnonymousElement): Option[Long] = {
    val res = store.get(Encoder.encodeAnonymousElementKey(collectionId, entity.identifier))
    if (res.nonEmpty) {
      Some(entity.identifier)
    } else {
      None
    }
  }

  def newAnonymousElement(store: KeyValueStore, l: Long): AnonymousElement = {
    ???
  }

  private def fetchOrCreateAnonymousElement(store: KeyValueStore, collectionId: Long, entity: AnonymousElement): (AnonymousElement, Long) = {
    val res = ReadOperations.fetchAnonymousElementId(store, collectionId, entity)
    if (res.isEmpty) {
      createAnonymousElement(store, collectionId, entity)
    } else {
      (entity, res.get)
    }
  }

  private def createAnonymousElement(store: KeyValueStore, collectionId: Long, entity: AnonymousElement): (AnonymousElement, Long) = {
    //TODO get next id
    //TODO write to AnonymousEntities
    ???
  }

  private def fetchOrCreateContext(store: KeyValueStore, collectionId: Long, context: Context): (Context, Long) = {
    val res = ReadOperations.fetchContextId(store, collectionId, context)
    if (res.isEmpty) {
      createContext(store, collectionId, context)
    } else {
      (context, res.get)
    }
  }

  private def createContext(store: KeyValueStore, collectionId: Long, context: Context): (Context, Long) = {
    //TODO get next id
    //TODO write to Contexts
    ???
  }

}
