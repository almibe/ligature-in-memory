/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import dev.ligature.{AnonymousEntity, Entity, NamedEntity, Object, Predicate, Statement, StringLiteral, Range}
import scodec.bits.{BitVector, ByteVector}
import scodec.{Attempt, Codec, DecodeResult, SizeBound}
import scodec.codecs.{byte, long, utf8}
import scodec.codecs.implicits.{implicitOptionCodec => _, implicitStringCodec => _, _}

object Encoder {
  private implicit val utf: Codec[String] = scodec.codecs.utf8

  private implicit val x: Codec[Option[Long]] = new Codec[Option[Long]] {
    override def decode(bits: BitVector): Attempt[DecodeResult[Option[Long]]] = ???
    override def encode(value: Option[Long]): Attempt[BitVector] = {
      value match {
        case Some(a) => long(64).encode(a)
        case None => Attempt.Successful(BitVector.empty)
      }
    }
    override def sizeBound: SizeBound = SizeBound.unknown
  }

  private implicit val y: Codec[Option[ObjectEncoding]] = new Codec[Option[ObjectEncoding]] {
    override def decode(bits: BitVector): Attempt[DecodeResult[Option[ObjectEncoding]]] = ???
    override def encode(value: Option[ObjectEncoding]): Attempt[BitVector] = {
      value match {
        case Some(a) => Codec.encode(a)
        case None => Attempt.Successful(BitVector.empty)
      }
    }
    override def sizeBound: SizeBound = SizeBound.unknown
  }

  val collectionNamesPrefixStart: ByteVector = Codec.encode(Prefixes.CollectionNameToId).require.bytes
  val empty: ByteVector = ByteVector.empty

  private case class CollectionNameToIdKey(prefix: Byte, collectionName: String)
  def encodeCollectionNameToIdKey(collectionName: NamedEntity): ByteVector =
    Codec.encode(CollectionNameToIdKey(Prefixes.CollectionNameToId, collectionName.identifier)).require.bytes
  private case class CollectionNameToIdValue(collectionId: Long)
  def encodeCollectionNameToIdValue(id: Long): ByteVector =
    Codec.encode(CollectionNameToIdValue(id)).require.bytes

  private case class IdToCollectionNameKey(prefix: Byte, collectionId: Long)
  def encodeIdToCollectionNameKey(id: Long): ByteVector =
    Codec.encode(IdToCollectionNameKey(Prefixes.IdToCollectionName, id)).require.bytes
  private case class IdToCollectionNameValue(collectionName: String)
  def encodeIdToCollectionNameValue(name: NamedEntity): ByteVector =
    Codec.encode(IdToCollectionNameValue(name.identifier)).require.bytes

  private case class CollectionNameCounterKey(prefix: Byte)
  def encodeCollectionNameCounterKey(): ByteVector =
    Codec.encode(CollectionNameCounterKey(Prefixes.CollectionNameCounter)).require.bytes
  private case class CollectionNameCounterValue(counter: Long)
  def encodeCollectionNameCounterValue(counter: Long): ByteVector =
    Codec.encode(CollectionNameCounterValue(counter)).require.bytes

  def encodeSubject(entity: Entity): ByteVector = ???
  case class ObjectEncoding(`type`: Byte, id: Long)
  def encodeObject(obj: Object): ByteVector = ???

  case class NamedEntitiesToIdKey(prefix: Byte, collectionId: Long, namedEntity: String)
  def encodeNamedEntitiesToIdKey(collectionId: Long, entity: NamedEntity): ByteVector = {
    Codec.encode(NamedEntitiesToIdKey(Prefixes.NamedEntitiesToId, collectionId, entity.identifier)).require.bytes
  }

  def encodeNamedEntitiesToIdValue(nextId: Long): ByteVector = {
    long(64).encode(nextId).require.bytes
  }

  case class IdToNamedEntitiesKey(prefix: Byte, collectionId: Long, entity: Long)
  def encodeIdToNamedEntitiesKey(collectionId: Long, entity: Long): ByteVector = {
    Codec.encode(IdToNamedEntitiesKey(Prefixes.IdToNamedEntities, collectionId, entity)).require.bytes
  }

  def encodeIdToNamedEntitiesValue(entity: NamedEntity): ByteVector = {
    utf8.encode(entity.identifier).require.bytes
  }

  case class PredicatesToIdKey(prefix: Byte, collectionId: Long, predicate: String)
  def encodePredicatesToIdKey(collectionId: Long, predicate: Predicate): ByteVector = {
    Codec.encode(PredicatesToIdKey(Prefixes.PredicatesToId, collectionId, predicate.identifier)).require.bytes
  }

  def encodePredicatesToIdValue(nextId: Long): ByteVector = {
    long(64).encode(nextId).require.bytes
  }

  case class IdToPredicatesKey(prefix: Byte, collectionId: Long, predicateId: Long)
  def encodeIdToPredicatesKey(collectionId: Long, predicateId: Long): ByteVector = {
    Codec.encode(IdToPredicatesKey(Prefixes.IdToPredicates, collectionId, predicateId)).require.bytes
  }

  def encodeIdToPredicatesValue(predicate: Predicate): ByteVector = {
    utf8.encode(predicate.identifier).require.bytes
  }

  case class StringToIdKey(prefix: Byte, collectionId: Long, stringLiteral: String)
  def encodeStringToIdKey(collectionId: Long, stringLiteral: StringLiteral): ByteVector = {
    Codec.encode(StringToIdKey(Prefixes.StringToId, collectionId, stringLiteral.value)).require.bytes
  }

  def encodeStringToIdValue(nextId: Long): ByteVector = {
    long(64).encode(nextId).require.bytes
  }

  case class IdToStringKey(prefix: Byte, collectionId: Long, stringLiteralId: Long)
  def encodeIdToStringKey(collectionId: Long, stringLiteralId: Long): ByteVector = {
    Codec.encode(IdToStringKey(Prefixes.IdToPredicates, collectionId, stringLiteralId)).require.bytes
  }

  def encodeIdToStringValue(stringLiteral: StringLiteral): ByteVector = {
    utf8.encode(stringLiteral.value).require.bytes
  }

  def encodeStatement(collectionId: Long, statement: Statement): Seq[ByteVector] = ???

  case class SPO(prefix: Byte,
                 collectionId: Long,
                 subject: Option[ObjectEncoding],
                 predicateId: Option[Long],
                 `object`: Option[ObjectEncoding])
  def encodeSPOPrefix(collectionId: Long,
                      subject: Option[ObjectEncoding],
                      predicate: Option[Long],
                      `object`: Option[ObjectEncoding]): ByteVector = {
    Codec.encode(SPO(Prefixes.SPOC, collectionId, subject, predicate, `object`)).require.bytes
  }

  case class SOP(prefix: Byte,
                 collectionId: Long,
                 subject: Option[ObjectEncoding],
                 `object`: Option[ObjectEncoding],
                 predicateId: Option[Long])
  def encodeSOPPrefix(collectionId: Long,
                              subject: Option[ObjectEncoding],
                              predicate: Option[Long],
                              `object`: Option[ObjectEncoding]): ByteVector = {
    Codec.encode(SOP(Prefixes.SOPC, collectionId, subject, `object`, predicate)).require.bytes
  }

  case class PSO(prefix: Byte,
                 collectionId: Long,
                 predicateId: Option[Long],
                 subject: Option[ObjectEncoding],
                 `object`: Option[ObjectEncoding])
  def encodePSOPrefix(collectionId: Long,
                      subject: Option[ObjectEncoding],
                      predicate: Option[Long],
                      `object`: Option[ObjectEncoding]): ByteVector = {
    Codec.encode(PSO(Prefixes.PSOC, collectionId, predicate, subject, `object`)).require.bytes
  }

  case class POS(prefix: Byte,
                 collectionId: Long,
                 predicateId: Option[Long],
                 `object`: Option[ObjectEncoding],
                 subject: Option[ObjectEncoding])
  def encodePOSPrefix(collectionId: Long,
                      subject: Option[ObjectEncoding],
                      predicate: Option[Long],
                      `object`: Option[ObjectEncoding]): ByteVector = {
    Codec.encode(POS(Prefixes.POSC, collectionId, predicate, `object`, subject)).require.bytes
  }

  case class OSP(prefix: Byte,
                 collectionId: Long,
                 `object`: Option[ObjectEncoding],
                 subject: Option[ObjectEncoding],
                 predicateId: Option[Long])
  def encodeOSPPrefix(collectionId: Long,
                      subject: Option[ObjectEncoding],
                      predicate: Option[Long],
                      `object`: Option[ObjectEncoding]): ByteVector = {
    Codec.encode(OSP(Prefixes.OSPC, collectionId, `object`, subject, predicate)).require.bytes
  }

  case class OPS(prefix: Byte,
                 collectionId: Long,
                 `object`: Option[ObjectEncoding],
                 predicateId: Option[Long],
                 subject: Option[ObjectEncoding])
  def encodeOPSPrefix(collectionId: Long,
                      subject: Option[ObjectEncoding],
                      predicate: Option[Long],
                      `object`: Option[ObjectEncoding]): ByteVector = {
    Codec.encode(OPS(Prefixes.OPSC, collectionId, `object`, predicate, subject)).require.bytes
  }

//  def encodeSOPStartStop(collectionId: Long,
//                         subject: Option[ObjectEncoding],
//                         predicate: Option[Long],
//                         literalRange: Range[_]): (ByteVector, ByteVector) = {
//    ???
//  }
//
//  def encodePOSStartStop(collectionId: Long,
//                         subject: Option[ObjectEncoding],
//                         predicate: Option[Long],
//                         literalRange: Range[_]): (ByteVector, ByteVector) = {
//    ???
//  }
//
//  def encodeOSPStartStop(collectionId: Long,
//                         subject: Option[ObjectEncoding],
//                         predicate: Option[Long],
//                         literalRange: Range[_]): (ByteVector, ByteVector) = {
//    ???
//  }
//
//  def encodeOPSStartStop(collectionId: Long,
//                         subject: Option[ObjectEncoding],
//                         predicate: Option[Long],
//                         literalRange: Range[_]): (ByteVector, ByteVector) = {
//    ???
//  }

  case class SPOC(prefix: Byte,
                  collectionId: Long,
                  subject: ObjectEncoding,
                  predicateId: Long,
                  `object`: ObjectEncoding,
                  context: Long)
  def encodeSPOC(collectionId: Long, subject: ObjectEncoding, predicateId: Long,
                 obj: ObjectEncoding, context: AnonymousEntity): ByteVector = {
    Codec.encode(SPOC(Prefixes.SPOC, collectionId, subject, predicateId, obj, context.identifier)).require.bytes
  }

  case class SOPC(prefix: Byte,
                  collectionId: Long,
                  subject: ObjectEncoding,
                  `object`: ObjectEncoding,
                  predicateId: Long,
                  context: Long)
  def encodeSOPC(collectionId: Long, subject: ObjectEncoding, predicateId: Long,
                 obj: ObjectEncoding, context: AnonymousEntity): ByteVector = {
    Codec.encode(SOPC(Prefixes.SOPC, collectionId, subject, obj, predicateId, context.identifier)).require.bytes
  }

  case class PSOC(prefix: Byte,
                  collectionId: Long,
                  predicateId: Long,
                  subject: ObjectEncoding,
                  `object`: ObjectEncoding,
                  context: Long)
  def encodePSOC(collectionId: Long, subject: ObjectEncoding, predicateId: Long,
                 obj: ObjectEncoding, context: AnonymousEntity): ByteVector = {
    Codec.encode(PSOC(Prefixes.PSOC, collectionId, predicateId, subject, obj, context.identifier)).require.bytes
  }

  case class POSC(prefix: Byte,
                  collectionId: Long,
                  predicateId: Long,
                  `object`: ObjectEncoding,
                  subject: ObjectEncoding,
                  context: Long)
  def encodePOSC(collectionId: Long, subject: ObjectEncoding, predicateId: Long,
                 obj: ObjectEncoding, context: AnonymousEntity): ByteVector = {
    Codec.encode(POSC(Prefixes.POSC, collectionId, predicateId, obj, subject, context.identifier)).require.bytes
  }

  case class OSPC(prefix: Byte,
                  collectionId: Long,
                  `object`: ObjectEncoding,
                  subject: ObjectEncoding,
                  predicateId: Long,
                  context: Long)
  def encodeOSPC(collectionId: Long, subject: ObjectEncoding, predicateId: Long,
                 obj: ObjectEncoding, context: AnonymousEntity): ByteVector = {
    Codec.encode(OSPC(Prefixes.OSPC, collectionId, obj, subject, predicateId, context.identifier)).require.bytes
  }

  case class OPSC(prefix: Byte,
                  collectionId: Long,
                  `object`: ObjectEncoding,
                  predicateId: Long,
                  subject: ObjectEncoding,
                  context: Long)
  def encodeOPSC(collectionId: Long, subject: ObjectEncoding, predicateId: Long,
                 obj: ObjectEncoding, context: AnonymousEntity): ByteVector = {
    Codec.encode(OPSC(Prefixes.OPSC, collectionId, obj, predicateId, subject, context.identifier)).require.bytes
  }

  case class CSPO(prefix: Byte,
                  collectionId: Long,
                  context: Long,
                  subject: ObjectEncoding,
                  predicateId: Long,
                  `object`: ObjectEncoding)
  def encodeCSPO(collectionId: Long, subject: ObjectEncoding, predicateId: Long,
                 obj: ObjectEncoding, context: AnonymousEntity): ByteVector = {
    Codec.encode(CSPO(Prefixes.CSPO, collectionId, context.identifier, subject, predicateId, obj)).require.bytes
  }

  private val byteLong = byte ~~ long(64)
  def encodeSPOCScanStart(collectionId: Long): ByteVector = {
    byteLong.encode(Prefixes.SPOC, collectionId).require.bytes
  }

  private case class CollectionCounterKey(prefix: Byte, collectionId: Long)
  def encodeCollectionCounterKey(collectionId: Long): ByteVector = {
    Codec.encode(CollectionCounterKey(Prefixes.CollectionCounter, collectionId)).require.bytes
  }

  def encodeCollectionCounterValue(value: Long): ByteVector = {
    Codec.encode(value).require.bytes
  }

  private case class AnonymousEntityKey(prefix: Byte, collectionId: Long, anonymousId: Long)
  def encodeAnonymousEntityKey(collectionId: Long, anonymousId: Long): ByteVector = {
    Codec.encode(AnonymousEntityKey(Prefixes.AnonymousEntities, collectionId, anonymousId)).require.bytes
  }
}
