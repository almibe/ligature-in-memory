/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import dev.ligature.{Entity, NamedEntity, Object, Statement}
import scodec.bits.ByteVector
import scodec.{Encoder => E}

object Encoder {
  private case class CollectionNameToIdKey(prefix: Byte, collectionName: String)
  def encodeCollectionNameToIdKey(collectionName: NamedEntity): ByteVector =
    E.encode(CollectionNameToIdKey(Prefixes.CollectionNameToId, collectionName.identifier)).require.bytes
  private case class CollectionNameToIdValue(collectionId: Long)
  def encodeCollectionNameToIdValue(id: Long): ByteVector =
    E.encode(CollectionNameToIdValue(id)).require.bytes

  private case class IdToCollectionNameKey(prefix: Byte, collectionId: Long)
  def encodeIdToCollectionNameKey(id: Long): ByteVector =
    E.encode(IdToCollectionNameKey(Prefixes.IdToCollectionName, id)).require.bytes
  private case class IdToCollectionNameValue(collectionName: String)
  def encodeIdToCollectionNameValue(name: NamedEntity): ByteVector =
    E.encode(IdToCollectionNameValue(name.identifier)).require.bytes

  private case class CollectionNameCounterKey(prefix: Byte)
  def encodeCollectionNameCounterKey(): ByteVector =
    E.encode(CollectionNameCounterKey(Prefixes.CollectionNameCounter)).require.bytes
  private case class CollectionNameCounterValue(counter: Long)
  def encodeCollectionNameCounterValue(counter: Long): ByteVector =
    E.encode(CollectionNameCounterValue(counter)).require.bytes

  private case class SubjectEncoding(`type`: Byte, id: Long)
  def encodeSubject(entity: Entity): ByteVector = ???
  private case class ObjectEncoding(`type`: Byte, id: Long)
  def encodeObject(obj: Object): ByteVector = ???

  def encodeStatement(collectionId: Long, statement: Statement): Seq[ByteVector] = ???

  private case class SPOC(prefix: Byte,
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
}
