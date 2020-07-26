/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import dev.ligature.store.keyvalue.Encoder.{CSPO, OPSC, OSPC, POSC, PSOC, SOPC, SPOC}
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs.implicits.{implicitStringCodec => _, _}

import scala.util.{Failure, Success, Try}

object Decoder {
  private implicit val utf: Codec[String] = scodec.codecs.utf8

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
      Success(SPOC(value.prefix, value.collectionId, value.subject, value.predicateId, value.`object`))
    } else {
      Failure(new RuntimeException("Invalid SOPC"))
    }
  }

  def decodePSOC(value: ByteVector): Try[SPOC] = {
    val res = Codec.decode[PSOC](value.bits)
    if (res.isSuccessful) {
      val value = res.require.value
      Success(SPOC(value.prefix, value.collectionId, value.subject, value.predicateId, value.`object`))
    } else {
      Failure(new RuntimeException("Invalid PSOC"))
    }
  }

  def decodePOSC(value: ByteVector): Try[SPOC] = {
    val res = Codec.decode[POSC](value.bits)
    if (res.isSuccessful) {
      val value = res.require.value
      Success(SPOC(value.prefix, value.collectionId, value.subject, value.predicateId, value.`object`))
    } else {
      Failure(new RuntimeException("Invalid POSC"))
    }
  }

  def decodeOSPC(value: ByteVector): Try[SPOC] = {
    val res = Codec.decode[OSPC](value.bits)
    if (res.isSuccessful) {
      val value = res.require.value
      Success(SPOC(value.prefix, value.collectionId, value.subject, value.predicateId, value.`object`))
    } else {
      Failure(new RuntimeException("Invalid OSPC"))
    }
  }

  def decodeOPSC(value: ByteVector): Try[SPOC] = {
    val res = Codec.decode[OPSC](value.bits)
    if (res.isSuccessful) {
      val value = res.require.value
      Success(SPOC(value.prefix, value.collectionId, value.subject, value.predicateId, value.`object`))
    } else {
      Failure(new RuntimeException("Invalid OPSC"))
    }
  }

  def decodeCSPO(value: ByteVector): Try[SPOC] = {
    val res = Codec.decode[CSPO](value.bits)
    if (res.isSuccessful) {
      val value = res.require.value
      Success(SPOC(value.prefix, value.collectionId, value.subject, value.predicateId, value.`object`))
    } else {
      Failure(new RuntimeException("Invalid CSPO"))
    }
  }
}
