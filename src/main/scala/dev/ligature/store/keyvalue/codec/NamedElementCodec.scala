/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.codec

import dev.ligature.NamedNode
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs.{long, utf8}

object NamedNodeCodec {
  case class NamedNodesToIdKey(prefix: Byte, collectionId: Long, namedElement: String)

  def encodeNamedNodesToIdKey(collectionId: Long, entity: NamedNode): ByteVector = {
    Codec.encode(NamedNodesToIdKey(Prefixes.NamedNodesToId, collectionId, entity.identifier)).require.bytes
  }

  def encodeNamedNodesToIdValue(nextId: Long): ByteVector = {
    long(64).encode(nextId).require.bytes
  }

  case class IdToNamedNodesKey(prefix: Byte, collectionId: Long, entity: Long)

  def encodeIdToNamedNodesKey(collectionId: Long, entity: Long): ByteVector = {
    Codec.encode(IdToNamedNodesKey(Prefixes.IdToNamedNodes, collectionId, entity)).require.bytes
  }

  def encodeIdToNamedNodesValue(entity: NamedNode): ByteVector = {
    utf8.encode(entity.identifier).require.bytes
  }

}
