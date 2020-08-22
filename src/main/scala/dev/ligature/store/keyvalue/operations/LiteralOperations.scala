/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.operations

import dev.ligature.{BooleanLiteral, DoubleLiteral, LangLiteral, StringLiteral}
import dev.ligature.store.keyvalue.KeyValueStore
import dev.ligature.store.keyvalue.codec.Encoder

object LiteralOperations {
  def decodeDoubleLiteral(literalId: Long): DoubleLiteral = {
    Decoder.decodeDoubleLiteral(literalId)
  }

  def decodeBooleanLiteral(literalId: Long): BooleanLiteral = {
    Decoder.decodeBooleanLiteral(literalId)
  }

  def handleStringLiteralLookup(store: KeyValueStore, collectionId: Long, literalId: Long): StringLiteral = {
    val res = store.get(Encoder.encodeIdToStringKey(collectionId, literalId))
    StringLiteral(Decoder.decodeStringLiteral(res.get))
  }

  def handleLangLiteralLookup(store: KeyValueStore, collectionId: Long, literalId: Long): LangLiteral = {
    //TODO lookup in IdToLangLiteral
    ???
  }

  def fetchLangLiteralId(store: KeyValueStore, collectionId: Long, langLiteral: LangLiteral): Option[Long] = {
    //TODO look up in LangLiteralToId
    //TODO return accordingly
    ???
  }

  def fetchStringLiteralId(store: KeyValueStore, collectionId: Long, stringLiteral: StringLiteral): Option[Long] = {
    val res = store.get(Encoder.encodeStringToIdKey(collectionId, stringLiteral))
    if (res.nonEmpty) {
      Some(res.get.toLong())
    } else {
      None
    }
  }

  private def fetchOrCreateLangLiteral(store: KeyValueStore, collectionId: Long, literal: LangLiteral): (Object, Long) = {
    val res = ReadOperations.fetchLangLiteralId(store, collectionId, literal)
    if (res.isEmpty) {
      createLangLiteral(store, collectionId, literal)
    } else {
      (literal, res.get)
    }
  }

  private def createLangLiteral(store: KeyValueStore, collectionId: Long, langLiteral: LangLiteral): (Object, Long) = {
    //TODO get next id
    //TODO write to LangLiteralToId
    //TODO write to IdToLangLiteral
    ???
  }

  private def fetchOrCreateDoubleLiteral(store: KeyValueStore, collectionId: Long, literal: DoubleLiteral): (Object, Long) = {
    //TODO not sure I need this since I'm storing doubles directly?
    ???
  }

  private def fetchOrCreateStringLiteral(store: KeyValueStore, collectionId: Long, literal: StringLiteral): (Object, Long) = {
    val res = ReadOperations.fetchStringLiteralId(store, collectionId, literal)
    if (res.isEmpty) {
      createStringLiteral(store, collectionId, literal)
    } else {
      (literal, res.get)
    }
  }

  private def createStringLiteral(store: KeyValueStore, collectionId: Long, stringLiteral: StringLiteral): (Object, Long) = {
    val nextId = nextCollectionId(store, collectionId)
    val stringToIdKey = Encoder.encodeStringToIdKey(collectionId, stringLiteral)
    val stringToIdValue = Encoder.encodeStringToIdValue(nextId)
    val idToStringKey = Encoder.encodeIdToStringKey(collectionId, nextId)
    val idToStringValue = Encoder.encodeIdToStringValue(stringLiteral)
    store.put(stringToIdKey, stringToIdValue)
    store.put(idToStringKey, idToStringValue)
    (stringLiteral, nextId)
  }

  private def fetchOrCreateBooleanLiteral(store: KeyValueStore, collectionId: Long, literal: BooleanLiteral): (Object, Long) = {
    //TODO not sure I need this since I'm storing booleans directly?
    ???
  }

}
