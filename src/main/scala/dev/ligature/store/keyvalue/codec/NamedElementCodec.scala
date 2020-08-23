/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.codec

import dev.ligature.NamedElement
import dev.ligature.store.keyvalue.Prefixes
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs.{long, utf8}

object NamedElementCodec {
  case class NamedElementsToIdKey(prefix: Byte, collectionId: Long, namedElement: String)

  def encodeNamedElementsToIdKey(collectionId: Long, entity: NamedElement): ByteVector = {
    Codec.encode(NamedElementsToIdKey(Prefixes.NamedElementsToId, collectionId, entity.identifier)).require.bytes
  }

  def encodeNamedElementsToIdValue(nextId: Long): ByteVector = {
    long(64).encode(nextId).require.bytes
  }

  case class IdToNamedElementsKey(prefix: Byte, collectionId: Long, entity: Long)

  def encodeIdToNamedElementsKey(collectionId: Long, entity: Long): ByteVector = {
    Codec.encode(IdToNamedElementsKey(Prefixes.IdToNamedElements, collectionId, entity)).require.bytes
  }

  def encodeIdToNamedElementsValue(entity: NamedElement): ByteVector = {
    utf8.encode(entity.identifier).require.bytes
  }

}
