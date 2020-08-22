/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.operations

import dev.ligature.NamedElement
import dev.ligature.store.keyvalue.KeyValueStore

object PredicateOperations {
  def lookupPredicate(store: KeyValueStore, collectionId: Long, predicate: Option[NamedElement]): Option[Long] = {
    predicate flatMap {
      fetchPredicateId(store, collectionId, _) flatMap {
        Some(_)
      }
    }
  }

}
