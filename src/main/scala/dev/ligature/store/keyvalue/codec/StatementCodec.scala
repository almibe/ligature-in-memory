/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.codec

import dev.ligature.{AnonymousNode, Statement}
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs.{byte, long}

import scala.util.{Failure, Success, Try}

object StatementCodec {
  def encodeStatement(collectionId: Long, statement: Statement): Seq[ByteVector] = ???

  case class SPO(prefix: Byte,
                 collectionId: Long,
                 subject: Option[ElementEncoding],
                 predicateId: Option[Long],
                 `object`: Option[ElementEncoding])

  def encodeSPOPrefix(collectionId: Long,
                      subject: Option[ElementEncoding],
                      predicate: Option[Long],
                      `object`: Option[ElementEncoding]): ByteVector = {
    Codec.encode(SPO(Prefixes.SPOC, collectionId, subject, predicate, `object`)).require.bytes
  }

  case class SOP(prefix: Byte,
                 collectionId: Long,
                 subject: Option[ElementEncoding],
                 `object`: Option[ElementEncoding],
                 predicateId: Option[Long])

  def encodeSOPPrefix(collectionId: Long,
                      subject: Option[ElementEncoding],
                      predicate: Option[Long],
                      `object`: Option[ElementEncoding]): ByteVector = {
    Codec.encode(SOP(Prefixes.SOPC, collectionId, subject, `object`, predicate)).require.bytes
  }

  case class PSO(prefix: Byte,
                 collectionId: Long,
                 predicateId: Option[Long],
                 subject: Option[ElementEncoding],
                 `object`: Option[ElementEncoding])

  def encodePSOPrefix(collectionId: Long,
                      subject: Option[ElementEncoding],
                      predicate: Option[Long],
                      `object`: Option[ElementEncoding]): ByteVector = {
    Codec.encode(PSO(Prefixes.PSOC, collectionId, predicate, subject, `object`)).require.bytes
  }

  case class POS(prefix: Byte,
                 collectionId: Long,
                 predicateId: Option[Long],
                 `object`: Option[ElementEncoding],
                 subject: Option[ElementEncoding])

  def encodePOSPrefix(collectionId: Long,
                      subject: Option[ElementEncoding],
                      predicate: Option[Long],
                      `object`: Option[ElementEncoding]): ByteVector = {
    Codec.encode(POS(Prefixes.POSC, collectionId, predicate, `object`, subject)).require.bytes
  }

  case class OSP(prefix: Byte,
                 collectionId: Long,
                 `object`: Option[ElementEncoding],
                 subject: Option[ElementEncoding],
                 predicateId: Option[Long])

  def encodeOSPPrefix(collectionId: Long,
                      subject: Option[ElementEncoding],
                      predicate: Option[Long],
                      `object`: Option[ElementEncoding]): ByteVector = {
    Codec.encode(OSP(Prefixes.OSPC, collectionId, `object`, subject, predicate)).require.bytes
  }

  case class OPS(prefix: Byte,
                 collectionId: Long,
                 `object`: Option[ElementEncoding],
                 predicateId: Option[Long],
                 subject: Option[ElementEncoding])

  def encodeOPSPrefix(collectionId: Long,
                      subject: Option[ElementEncoding],
                      predicate: Option[Long],
                      `object`: Option[ElementEncoding]): ByteVector = {
    Codec.encode(OPS(Prefixes.OPSC, collectionId, `object`, predicate, subject)).require.bytes
  }

  //  def encodeSOPStartStop(collectionId: Long,
  //                         subject: Option[ElementEncoding],
  //                         predicate: Option[Long],
  //                         literalRange: Range[_]): (ByteVector, ByteVector) = {
  //    ???
  //  }
  //
  //  def encodePOSStartStop(collectionId: Long,
  //                         subject: Option[ElementEncoding],
  //                         predicate: Option[Long],
  //                         literalRange: Range[_]): (ByteVector, ByteVector) = {
  //    ???
  //  }
  //
  //  def encodeOSPStartStop(collectionId: Long,
  //                         subject: Option[ElementEncoding],
  //                         predicate: Option[Long],
  //                         literalRange: Range[_]): (ByteVector, ByteVector) = {
  //    ???
  //  }
  //
  //  def encodeOPSStartStop(collectionId: Long,
  //                         subject: Option[ElementEncoding],
  //                         predicate: Option[Long],
  //                         literalRange: Range[_]): (ByteVector, ByteVector) = {
  //    ???
  //  }

  case class SPOC(prefix: Byte,
                  collectionId: Long,
                  subject: ElementEncoding,
                  predicateId: Long,
                  `object`: ElementEncoding,
                  context: Long)

  def encodeSPOC(collectionId: Long, subject: ElementEncoding, predicateId: Long,
                 obj: ElementEncoding, context: AnonymousNode): ByteVector = {
    Codec.encode(SPOC(Prefixes.SPOC, collectionId, subject, predicateId, obj, context.identifier)).require.bytes
  }

  case class SOPC(prefix: Byte,
                  collectionId: Long,
                  subject: ElementEncoding,
                  `object`: ElementEncoding,
                  predicateId: Long,
                  context: Long)

  def encodeSOPC(collectionId: Long, subject: ElementEncoding, predicateId: Long,
                 obj: ElementEncoding, context: AnonymousNode): ByteVector = {
    Codec.encode(SOPC(Prefixes.SOPC, collectionId, subject, obj, predicateId, context.identifier)).require.bytes
  }

  case class PSOC(prefix: Byte,
                  collectionId: Long,
                  predicateId: Long,
                  subject: ElementEncoding,
                  `object`: ElementEncoding,
                  context: Long)

  def encodePSOC(collectionId: Long, subject: ElementEncoding, predicateId: Long,
                 obj: ElementEncoding, context: AnonymousNode): ByteVector = {
    Codec.encode(PSOC(Prefixes.PSOC, collectionId, predicateId, subject, obj, context.identifier)).require.bytes
  }

  case class POSC(prefix: Byte,
                  collectionId: Long,
                  predicateId: Long,
                  `object`: ElementEncoding,
                  subject: ElementEncoding,
                  context: Long)

  def encodePOSC(collectionId: Long, subject: ElementEncoding, predicateId: Long,
                 obj: ElementEncoding, context: AnonymousNode): ByteVector = {
    Codec.encode(POSC(Prefixes.POSC, collectionId, predicateId, obj, subject, context.identifier)).require.bytes
  }

  case class OSPC(prefix: Byte,
                  collectionId: Long,
                  `object`: ElementEncoding,
                  subject: ElementEncoding,
                  predicateId: Long,
                  context: Long)

  def encodeOSPC(collectionId: Long, subject: ElementEncoding, predicateId: Long,
                 obj: ElementEncoding, context: AnonymousNode): ByteVector = {
    Codec.encode(OSPC(Prefixes.OSPC, collectionId, obj, subject, predicateId, context.identifier)).require.bytes
  }

  case class OPSC(prefix: Byte,
                  collectionId: Long,
                  `object`: ElementEncoding,
                  predicateId: Long,
                  subject: ElementEncoding,
                  context: Long)

  def encodeOPSC(collectionId: Long, subject: ElementEncoding, predicateId: Long,
                 obj: ElementEncoding, context: AnonymousNode): ByteVector = {
    Codec.encode(OPSC(Prefixes.OPSC, collectionId, obj, predicateId, subject, context.identifier)).require.bytes
  }

  case class CSPO(prefix: Byte,
                  collectionId: Long,
                  context: Long,
                  subject: ElementEncoding,
                  predicateId: Long,
                  `object`: ElementEncoding)

  def encodeCSPO(collectionId: Long, subject: ElementEncoding, predicateId: Long,
                 obj: ElementEncoding, context: AnonymousNode): ByteVector = {
    Codec.encode(CSPO(Prefixes.CSPO, collectionId, context.identifier, subject, predicateId, obj)).require.bytes
  }

  private val byteLong = byte ~~ long(64)

  def encodeSPOCScanStart(collectionId: Long): ByteVector = {
    byteLong.encode(Prefixes.SPOC, collectionId).require.bytes
  }


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
}
