/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

case class CollectionNameToIdKey(prefix: Byte, collectionName: String)
case class CollectionNameToIdValue(collectionId: Long)

case class IdToCollectionNameKey(prefix: Byte, collectionId: Long)
case class IdToCollectionNameValue(collectionName: String)

case class CollectionNameCounterKey(prefix: Byte)
case class CollectionNameCounterValue(counter: Long)

case class SubjectEncoding(`type`: Byte, id: Long)
case class ObjectEncoding(`type`: Byte, id: Long)

case class SPOC(prefix: Byte,
                collectionId: Long,
                subject: SubjectEncoding,
                predicateId: Long,
                `object`: ObjectEncoding,
                context: Long)

case class SOPC(prefix: Byte,
                collectionId: Long,
                subject: SubjectEncoding,
                `object`: ObjectEncoding,
                predicateId: Long,
                context: Long)

case class PSOC(prefix: Byte,
                collectionId: Long,
                predicateId: Long,
                subject: SubjectEncoding,
                `object`: ObjectEncoding,
                context: Long)

case class POSC(prefix: Byte,
                collectionId: Long,
                predicateId: Long,
                `object`: ObjectEncoding,
                subject: SubjectEncoding,
                context: Long)

case class OSPC(prefix: Byte,
                collectionId: Long,
                `object`: ObjectEncoding,
                subject: SubjectEncoding,
                predicateId: Long,
                context: Long)

case class OPSC(prefix: Byte,
                collectionId: Long,
                `object`: ObjectEncoding,
                predicateId: Long,
                subject: SubjectEncoding,
                context: Long)

case class CSPO(prefix: Byte,
                collectionId: Long,
                context: Long,
                subject: SubjectEncoding,
                predicateId: Long,
                `object`: ObjectEncoding)
