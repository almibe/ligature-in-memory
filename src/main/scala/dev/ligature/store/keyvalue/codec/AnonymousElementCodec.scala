/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.codec

import scodec.Codec
import scodec.bits.ByteVector

object AnonymousElementCodec {
  private case class AnonymousElementKey(prefix: Byte, collectionId: Long, anonymousId: Long)

  def encodeAnonymousElementKey(collectionId: Long, anonymousId: Long): ByteVector = {
    Codec.encode(AnonymousElementKey(Prefixes.AnonymousElements, collectionId, anonymousId)).require.bytes
  }
}
