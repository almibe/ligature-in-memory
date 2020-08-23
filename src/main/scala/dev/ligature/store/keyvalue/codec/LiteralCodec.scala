/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.codec

import dev.ligature.{BooleanLiteral, DoubleLiteral, StringLiteral}
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs.{long, utf8}

object LiteralCodec {
  case class StringToIdKey(prefix: Byte, collectionId: Long, stringLiteral: String)

  def encodeStringToIdKey(collectionId: Long, stringLiteral: StringLiteral): ByteVector = {
    Codec.encode(StringToIdKey(Prefixes.StringToId, collectionId, stringLiteral.value)).require.bytes
  }

  def encodeStringToIdValue(nextId: Long): ByteVector = {
    long(64).encode(nextId).require.bytes
  }

  case class IdToStringKey(prefix: Byte, collectionId: Long, stringLiteralId: Long)

  def encodeIdToStringKey(collectionId: Long, stringLiteralId: Long): ByteVector = {
    Codec.encode(IdToStringKey(Prefixes.IdToNamedElements, collectionId, stringLiteralId)).require.bytes
  }

  def encodeIdToStringValue(stringLiteral: StringLiteral): ByteVector = {
    utf8.encode(stringLiteral.value).require.bytes
  }

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
