/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import dev.ligature.{Entity, NamedEntity, Object, Statement}
import scodec.bits.ByteVector
import scodec.Codec
import scodec.codecs.{byte, long, utf8}
import scodec.codecs.implicits.{implicitStringCodec => _, _}

object Encoder {
  private implicit val utf: Codec[String] = scodec.codecs.utf8
  val collectionNamesPrefixStart: ByteVector = Codec.encode(Prefixes.CollectionNameToId).require.bytes
  val collectionNamesPrefixEnd: ByteVector = Codec.encode((Prefixes.CollectionNameToId + 1).toByte).require.bytes
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

  case class SubjectEncoding(`type`: Byte, id: Long)
  def encodeSubject(entity: Entity): ByteVector = ???
  case class ObjectEncoding(`type`: Byte, id: Long)
  def encodeObject(obj: Object): ByteVector = ???

  case class NamedEntitiesKey(prefix: Byte, collectionId: Long, namedEntity: String)
  def encodeNamedEntitiesKey(collectionId: Long, entity: NamedEntity): ByteVector = {
    Codec.encode(Prefixes.NamedEntitiesToId, collectionId, entity.identifier).require.bytes
  }

  case class NamedEntitiesToIdKey(prefix: Byte, collectionId: Long, namedEntity: String)
  def encodeNamedEntitiesToIdKey(collectionId: Long, entity: NamedEntity): ByteVector = {
    Codec.encode(NamedEntitiesToIdKey(Prefixes.NamedEntitiesToId, collectionId, entity.identifier)).require.bytes
  }

  def encodeNamedEntitiesToIdValue(nextId: Long): ByteVector = {
    long(64).encode(nextId).require.bytes
  }

  case class IdToNamedEntitiesKey(prefix: Byte, collectionId: Long, anonymousEntity: Long)
  def encodeIdToNamedEntitiesKey(collectionId: Long, anonymousEntity: Long): ByteVector = {
    Codec.encode(IdToNamedEntitiesKey(Prefixes.IdToNamedEntities, collectionId, anonymousEntity)).require.bytes
  }

  def encodeIdToNamedEntitiesValue(entity: NamedEntity): ByteVector = {
    utf8.encode(entity.identifier).require.bytes
  }

  def encodeStatement(collectionId: Long, statement: Statement): Seq[ByteVector] = ???

  case class SPOC(prefix: Byte,
                          collectionId: Long,
                          subject: SubjectEncoding,
                          predicateId: Long,
                          `object`: ObjectEncoding,
                          context: Long)
  private def encodeSPOC(): ByteVector = ???

  private case class SOPC(prefix: Byte,
                          collectionId: Long,
                          subject: SubjectEncoding,
                          `object`: ObjectEncoding,
                          predicateId: Long,
                          context: Long)
  private def encodeSOPC(): ByteVector = ???

  private case class PSOC(prefix: Byte,
                          collectionId: Long,
                          predicateId: Long,
                          subject: SubjectEncoding,
                          `object`: ObjectEncoding,
                          context: Long)
  private def encodePSOC(): ByteVector = ???

  private case class POSC(prefix: Byte,
                          collectionId: Long,
                          predicateId: Long,
                          `object`: ObjectEncoding,
                          subject: SubjectEncoding,
                          context: Long)
  private def encodePOSC(): ByteVector = ???

  private case class OSPC(prefix: Byte,
                          collectionId: Long,
                          `object`: ObjectEncoding,
                          subject: SubjectEncoding,
                          predicateId: Long,
                          context: Long)
  private def encodeOSPC(): ByteVector = ???

  private case class OPSC(prefix: Byte,
                          collectionId: Long,
                          `object`: ObjectEncoding,
                          predicateId: Long,
                          subject: SubjectEncoding,
                          context: Long)
  private def encodeOPSC(): ByteVector = ???

  private case class CSPO(prefix: Byte,
                          collectionId: Long,
                          context: Long,
                          subject: SubjectEncoding,
                          predicateId: Long,
                          `object`: ObjectEncoding)
  private def encodeCSPO(): ByteVector = ???

  private val byteLong = byte ~~ long(64)
  def encodeSPOCScanStart(collectionId: Long): ByteVector = {
    byteLong.encode(Prefixes.SPOC, collectionId).require.bytes
  }
  def encodeSPOCScanEnd(collectionId: Long): ByteVector = {
    byteLong.encode(Prefixes.SPOC, collectionId + 1L).require.bytes
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
