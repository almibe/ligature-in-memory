/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.codec

import dev.ligature.AnonymousNode
import scodec.Codec
import scodec.bits.ByteVector

object AnonymousNodeCodec {
  private case class AnonymousElementKey(prefix: Byte, collectionId: Long, anonymousId: Long)

  def encode(anonymousNode: AnonymousNode): ByteVector = {
    ???
  }

  def decode(byteVector: ByteVector): AnonymousNode = {
    ???
  }

  def encodeAnonymousElementKey(collectionId: Long, anonymousId: Long): ByteVector = {
    Codec.encode(AnonymousElementKey(Prefixes.AnonymousNodes, collectionId, anonymousId)).require.bytes
  }
}
