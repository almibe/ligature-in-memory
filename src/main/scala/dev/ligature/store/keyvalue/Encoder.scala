/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import dev.ligature.{AnonymousEntity, Entity, NamedEntity, Object, Predicate, Statement}
import scodec.bits.ByteVector
import scodec.Codec
import scodec.codecs.{byte, long, utf8}
import scodec.codecs.implicits.{implicitStringCodec => _, _}

object Encoder {
  private implicit val utf: Codec[String] = scodec.codecs.utf8
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
    Codec.encode(IdToNamedEntitiesKey(Prefixes.IdToPredicates, collectionId, predicateId)).require.bytes
  }

  def encodeIdToPredicatesValue(predicate: Predicate): ByteVector = {
    utf8.encode(predicate.identifier).require.bytes
  }

  def encodeStatement(collectionId: Long, statement: Statement): Seq[ByteVector] = ???

  case class SPO(prefix: Byte,
                 collectionId: Long,
                 subject: ObjectEncoding,
                 predicateId: Long,
                 `object`: ObjectEncoding)
  def encodeSPOPrefix(collectionId: Long,
                      subject: Option[Entity],
                      predicate: Option[Predicate],
                      `object`: Option[Object]): ByteVector = ???

  case class SOP(prefix: Byte,
                 collectionId: Long,
                 subject: ObjectEncoding,
                 `object`: ObjectEncoding,
                 predicateId: Long)
  def encodeSOPPrefix(collectionId: Long,
                              subject: Option[Entity],
                              predicate: Option[Predicate],
                              `object`: Option[Object]): ByteVector = ???

  case class PSO(prefix: Byte,
                 collectionId: Long,
                 predicateId: Long,
                 subject: ObjectEncoding,
                 `object`: ObjectEncoding)
  def encodePSOPrefix(collectionId: Long,
                      subject: Option[Entity],
                      predicate: Option[Predicate],
                      `object`: Option[Object]): ByteVector = ???

  case class POS(prefix: Byte,
                 collectionId: Long,
                 predicateId: Long,
                 `object`: ObjectEncoding,
                 subject: ObjectEncoding)
  def encodePOSPrefix(collectionId: Long,
                      subject: Option[Entity],
                      predicate: Option[Predicate],
                      `object`: Option[Object]): ByteVector = ???

  case class OSP(prefix: Byte,
                 collectionId: Long,
                 `object`: ObjectEncoding,
                 subject: ObjectEncoding,
                 predicateId: Long)
  def encodeOSPPrefix(collectionId: Long,
                      subject: Option[Entity],
                      predicate: Option[Predicate],
                      `object`: Option[Object]): ByteVector = ???

  case class OPS(prefix: Byte,
                 collectionId: Long,
                 `object`: ObjectEncoding,
                 predicateId: Long,
                 subject: ObjectEncoding)
  def encodeOPSPrefix(collectionId: Long,
                      subject: Option[Entity],
                      predicate: Option[Predicate],
                      `object`: Option[Object]): ByteVector = ???

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
