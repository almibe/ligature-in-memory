/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.operations

import dev.ligature.NamedElement
import dev.ligature.store.keyvalue.KeyValueStore
import dev.ligature.store.keyvalue.codec.Encoder

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

}
