/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.operations

import dev.ligature.NamedElement
import dev.ligature.store.keyvalue.KeyValueStore
import dev.ligature.store.keyvalue.codec.Encoder
import .removePersistedStatement

import scala.util.{Success, Try}

object PredicateOperations {
  def lookupPredicate(store: KeyValueStore, collectionId: Long, predicate: Option[NamedElement]): Option[Long] = {
    predicate flatMap {
      fetchPredicateId(store, collectionId, _) flatMap {
        Some(_)
      }
    }
  }

  def removePredicate(store: KeyValueStore, collectionName: NamedElement, predicate: Predicate): Try[Predicate] = {
    //TODO check collection exists to short circuit
    val predicateMatches = ReadOperations.matchStatementsImpl(store, collectionName, None, Some(predicate))
    predicateMatches.foreach { s =>
      removePersistedStatement(store, s)
    }
    Success(predicate)
  }

  private def fetchOrCreatePredicate(store: KeyValueStore, collectionId: Long, predicate: Predicate): (Predicate, Long) = {
    val res = ReadOperations.fetchPredicateId(store, collectionId, predicate)
    if (res.isEmpty) {
      createPredicate(store, collectionId, predicate)
    } else {
      (predicate, res.get)
    }
  }

  private def createPredicate(store: KeyValueStore, collectionId: Long, predicate: Predicate): (Predicate, Long) = {
    val nextId = nextCollectionId(store, collectionId)
    val predicatesToIdKey = Encoder.encodePredicatesToIdKey(collectionId, predicate)
    val predicatesToIdValue = Encoder.encodePredicatesToIdValue(nextId)
    val idToPredicatesKey = Encoder.encodeIdToPredicatesKey(collectionId, nextId)
    val idToPredicatesValue = Encoder.encodeIdToPredicatesValue(predicate)
    store.put(predicatesToIdKey, predicatesToIdValue)
    store.put(idToPredicatesKey, idToPredicatesValue)
    (predicate, nextId)
  }


}
