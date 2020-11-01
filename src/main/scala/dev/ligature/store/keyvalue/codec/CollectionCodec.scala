/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.codec

import dev.ligature.NamedElement
import scodec.Codec
import scodec.bits.ByteVector

object CollectionCodec {
  private case class CollectionCounterKey(prefix: Byte, collectionId: Long)

  def encodeCollectionCounterKey(collectionId: Long): ByteVector = {
    Codec.encode(CollectionCounterKey(Prefixes.CollectionCounter, collectionId)).require.bytes
  }

  def encodeCollectionCounterValue(value: Long): ByteVector = {
    Codec.encode(value).require.bytes
  }

  val collectionNamesPrefixStart: ByteVector = Codec.encode(Prefixes.CollectionNameToId).require.bytes

  private case class CollectionNameToIdKey(prefix: Byte, collectionName: String)

  def encodeCollectionNameToIdKey(collectionName: NamedElement): ByteVector =
    Codec.encode(CollectionNameToIdKey(Prefixes.CollectionNameToId, collectionName.identifier)).require.bytes

  private case class CollectionNameToIdValue(collectionId: Long)

  def encodeCollectionNameToIdValue(id: Long): ByteVector =
    Codec.encode(CollectionNameToIdValue(id)).require.bytes

  private case class IdToCollectionNameKey(prefix: Byte, collectionId: Long)

  def encodeIdToCollectionNameKey(id: Long): ByteVector =
    Codec.encode(IdToCollectionNameKey(Prefixes.IdToCollectionName, id)).require.bytes

  private case class IdToCollectionNameValue(collectionName: String)

  def encodeIdToCollectionNameValue(name: NamedElement): ByteVector =
    Codec.encode(IdToCollectionNameValue(name.identifier)).require.bytes

  private case class CollectionNameCounterKey(prefix: Byte)

  def encodeCollectionNameCounterKey(): ByteVector =
    Codec.encode(CollectionNameCounterKey(Prefixes.CollectionNameCounter)).require.bytes

  private case class CollectionNameCounterValue(counter: Long)

  def encodeCollectionNameCounterValue(counter: Long): ByteVector =
    Codec.encode(CollectionNameCounterValue(counter)).require.bytes

}
