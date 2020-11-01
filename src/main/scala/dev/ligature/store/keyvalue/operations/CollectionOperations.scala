/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.operations

import dev.ligature.NamedElement
import dev.ligature.store.keyvalue.KeyValueStore
import dev.ligature.store.keyvalue.codec.CollectionCodec

import scala.util.{Success, Try}

object CollectionOperations {
  def collections(store: KeyValueStore): Iterable[NamedElement] = {
    val collectionNameToId = store.prefix(CollectionCodec.collectionNamesPrefixStart)
    collectionNameToId.map { encoded =>
      encoded._1.drop(1).decodeUtf8.map(NamedElement).getOrElse(throw new RuntimeException("Invalid Name"))
    }
  }

  def fetchCollectionId(store: KeyValueStore, collectionName: NamedElement): Option[Long] = {
    val encoded = CollectionCodec.encodeCollectionNameToIdKey(collectionName)
    val res = store.get(encoded)
    res.map(_.toLong())
  }

  def createCollection(store: KeyValueStore, collection: NamedElement): Try[Long] = {
    val id = fetchCollectionId(store, collection)
    if (id.isEmpty) {
      val nextId = nextCollectionNameId(store)
      val collectionNameToIdKey = CollectionCodec.encodeCollectionNameToIdKey(collection)
      val idToCollectionNameKey = CollectionCodec.encodeIdToCollectionNameKey(nextId)
      val collectionNameToIdValue = CollectionCodec.encodeCollectionNameToIdValue(nextId)
      val idToCollectionNameValue = CollectionCodec.encodeIdToCollectionNameValue(collection)
      store.put(collectionNameToIdKey, collectionNameToIdValue)
      store.put(idToCollectionNameKey, idToCollectionNameValue)
      Success(nextId)
    } else {
      Success(id.get)
    }
  }

  def deleteCollection(store: KeyValueStore, collection: NamedElement): Try[NamedElement] = {
    ???
//    val id = fetchCollectionId(store, collection)
//    if (id.nonEmpty) {
//      val collectionNameToIdKey = CollectionCodec.encodeCollectionNameToIdKey(collection)
//      val idToCollectionNameKey = CollectionCodec.encodeIdToCollectionNameKey(id.get)
//      store.delete(collectionNameToIdKey)
//      store.delete(idToCollectionNameKey)
//      Range(Prefixes.SPOC, Prefixes.IdToString + 1).foreach { prefix =>
//        store.delete((byte ~~ long(64)).encode((prefix.toByte, id.get)).require.bytes)
//      }
//      Success(collection)
//    } else {
//      Success(collection)
//    }
  }

  def nextCollectionNameId(store: KeyValueStore): Long = {
    val currentId = store.get(CollectionCodec.encodeCollectionNameCounterKey())
    val nextId = currentId match {
      case Some(bv) => bv.toLong() + 1
      case None => 0
    }
    store.put(CollectionCodec.encodeCollectionNameCounterKey(), CollectionCodec.encodeCollectionNameCounterValue(nextId))
    nextId
  }

  def fetchOrCreateCollection(store: KeyValueStore, collectionName: NamedElement): Long = {
    val id = fetchCollectionId(store, collectionName)
    if (id.isEmpty) {
      createCollection(store, collectionName).get
    } else {
      id.get
    }
  }

  private[keyvalue] def nextCollectionId(store: KeyValueStore, collectionId: Long): Long = {
    val key = CollectionCodec.encodeCollectionCounterKey(collectionId)
    val collectionCounter = store.get(key)
    val counterValue = if (collectionCounter.nonEmpty) {
      collectionCounter.get.toLong() + 1L
    } else {
      0
    }
    store.put(key, CollectionCodec.encodeCollectionCounterValue(counterValue))
    counterValue
  }
}
