/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.codec

import dev.ligature.{BooleanLiteral, DoubleLiteral}
import scodec.bits.ByteVector
import scodec.codecs.utf8

class LiteralCodec {

  def decodeDoubleLiteral(literal: Long): DoubleLiteral = {
    ???
  }

  def decodeBooleanLiteral(literalId: Long): BooleanLiteral = {
    ???
  }

  def decodeStringLiteral(byteVector: ByteVector): String = {
    utf8.decode(byteVector.bits).require.value
  }
}
