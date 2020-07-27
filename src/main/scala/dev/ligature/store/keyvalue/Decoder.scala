/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import dev.ligature.{BooleanLiteral, DoubleLiteral}
import dev.ligature.store.keyvalue.Encoder.{CSPO, OPSC, OSPC, POSC, PSOC, SOPC, SPOC}
import scodec.Codec
import scodec.codecs.utf8
import scodec.bits.ByteVector
import scodec.codecs.implicits.{implicitStringCodec => _, _}

import scala.util.{Failure, Success, Try}

object Decoder {
  private implicit val utf: Codec[String] = utf8

  def decodeSPOC(value: ByteVector): Try[SPOC] = {
    val res = Codec.decode[SPOC](value.bits)
    if (res.isSuccessful) {
      Success(res.require.value)
    } else {
      Failure(new RuntimeException("Invalid SPOC"))
    }
  }

  def decodeSOPC(value: ByteVector): Try[SPOC] = {
    val res = Codec.decode[SOPC](value.bits)
    if (res.isSuccessful) {
      val value = res.require.value
      Success(SPOC(value.prefix, value.collectionId, value.subject, value.predicateId, value.`object`, value.context))
    } else {
      Failure(new RuntimeException("Invalid SOPC"))
    }
  }

  def decodePSOC(value: ByteVector): Try[SPOC] = {
    val res = Codec.decode[PSOC](value.bits)
    if (res.isSuccessful) {
      val value = res.require.value
      Success(SPOC(value.prefix, value.collectionId, value.subject, value.predicateId, value.`object`, value.context))
    } else {
      Failure(new RuntimeException("Invalid PSOC"))
    }
  }

  def decodePOSC(value: ByteVector): Try[SPOC] = {
    val res = Codec.decode[POSC](value.bits)
    if (res.isSuccessful) {
      val value = res.require.value
      Success(SPOC(value.prefix, value.collectionId, value.subject, value.predicateId, value.`object`, value.context))
    } else {
      Failure(new RuntimeException("Invalid POSC"))
    }
  }

  def decodeOSPC(value: ByteVector): Try[SPOC] = {
    val res = Codec.decode[OSPC](value.bits)
    if (res.isSuccessful) {
      val value = res.require.value
      Success(SPOC(value.prefix, value.collectionId, value.subject, value.predicateId, value.`object`, value.context))
    } else {
      Failure(new RuntimeException("Invalid OSPC"))
    }
  }

  def decodeOPSC(value: ByteVector): Try[SPOC] = {
    val res = Codec.decode[OPSC](value.bits)
    if (res.isSuccessful) {
      val value = res.require.value
      Success(SPOC(value.prefix, value.collectionId, value.subject, value.predicateId, value.`object`, value.context))
    } else {
      Failure(new RuntimeException("Invalid OPSC"))
    }
  }

  def decodeCSPO(value: ByteVector): Try[SPOC] = { //TODO I might not ever use this?
    val res = Codec.decode[CSPO](value.bits)
    if (res.isSuccessful) {
      val value = res.require.value
      Success(SPOC(value.prefix, value.collectionId, value.subject, value.predicateId, value.`object`, value.context))
    } else {
      Failure(new RuntimeException("Invalid CSPO"))
    }
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
