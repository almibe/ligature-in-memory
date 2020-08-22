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

}
