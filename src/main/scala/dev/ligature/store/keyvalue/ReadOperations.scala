/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import dev.ligature.store.keyvalue.Encoder.ObjectEncoding
import dev.ligature.{AnonymousEntity, BooleanLiteral, Context, DoubleLiteral, Entity, LangLiteral, LongLiteral, NamedEntity, Object, PersistedStatement, Predicate, Statement, StringLiteral}
import scodec.bits.ByteVector

object ReadOperations {
  def collections(store: KeyValueStore): Iterable[NamedEntity] = {
    val collectionNameToId = store.prefix(Encoder.collectionNamesPrefixStart)
    collectionNameToId.map { encoded =>
      encoded._1.drop(1).decodeUtf8.map(NamedEntity).getOrElse(throw new RuntimeException("Invalid Name"))
    }
  }

  def fetchCollectionId(store: KeyValueStore, collectionName: NamedEntity): Option[Long] = {
    val encoded = Encoder.encodeCollectionNameToIdKey(collectionName)
    val res = store.get(encoded)
    res.map(_.toLong())
  }

  def readAllStatements(store: KeyValueStore, collectionName: NamedEntity): Option[Iterable[PersistedStatement]] = {
    val collectionId = fetchCollectionId(store, collectionName)
    if (collectionId.nonEmpty) {
      val itr = store.prefix(Encoder.encodeSPOCScanStart(collectionId.get))
      Some(itr.map { entry: (ByteVector, ByteVector) =>
        val attempt = Decoder.decodeSPOC(entry._1)
        if (attempt.isSuccess) {
          val spoc = attempt.get
          spocToPersistedStatement(store, collectionName, collectionId.get, spoc)
        } else {
          ???
        }
      })
    } else {
      None
    }
  }

  def spocToPersistedStatement(store: KeyValueStore, collectionName: NamedEntity, collectionId: Long, spoc: Encoder.SPOC): PersistedStatement = {
    val subject = handleSubjectLookup(store, collectionId, spoc.subject.`type`, spoc.subject.id)
    val predicate = handlePredicateLookup(store, collectionId, spoc.predicateId)
    val obj = handleObjectLookup(store, collectionId, spoc.`object`.`type`, spoc.`object`.id)
    val context = Context(spoc.context)
    val statement = Statement(subject, predicate, obj)
    PersistedStatement(collectionName, statement, context)
  }

  def handleSubjectLookup(store: KeyValueStore, collectionId: Long, subjectType: Byte, subjectId: Long): Entity = {
    subjectType match {
      case TypeCodes.NamedEntity => handleNamedEntityLookup(store, collectionId, subjectId)
      case TypeCodes.AnonymousEntity => AnonymousEntity(subjectId)
      case _ => throw new RuntimeException(s"Illegal subject type $subjectType")
    }
  }

  def handleNamedEntityLookup(store: KeyValueStore, collectionId: Long, entityId: Long): NamedEntity = {
    val res = store.get(Encoder.encodeIdToNamedEntitiesKey(collectionId, entityId))
    if (res.nonEmpty) {
      NamedEntity(Decoder.decodeStringLiteral(res.get))
    } else {
      throw new RuntimeException(s"Not valid NamedEntity - $collectionId $entityId")
    }
  }

  def handlePredicateLookup(store: KeyValueStore, collectionId: Long, predicateId: Long): Predicate = {
    val res = store.get(Encoder.encodeIdToPredicatesKey(collectionId, predicateId))
    if (res.nonEmpty) {
      Predicate(Decoder.decodeStringLiteral(res.get))
    } else {
      throw new RuntimeException(s"Not valid Predicate - $collectionId $predicateId")
    }
  }

  def handleObjectLookup(store: KeyValueStore, collectionId: Long, objectType: Byte, objectId: Long): Object = {
    objectType match {
      case TypeCodes.NamedEntity => handleNamedEntityLookup(store, collectionId, objectId)
      case TypeCodes.AnonymousEntity => AnonymousEntity(objectId)
      case TypeCodes.Double => decodeDoubleLiteral(objectId)
      case TypeCodes.Long => LongLiteral(objectId)
      case TypeCodes.Boolean => decodeBooleanLiteral(objectId)
      case TypeCodes.String => handleStringLiteralLookup(store, collectionId, objectId)
      case TypeCodes.LangLiteral => handleLangLiteralLookup(store, collectionId, objectId)
      case _ => throw new RuntimeException(s"Illegal object type $objectType")
    }
  }

  def decodeDoubleLiteral(literalId: Long): DoubleLiteral = {
    Decoder.decodeDoubleLiteral(literalId)
  }

  def decodeBooleanLiteral(literalId: Long): BooleanLiteral = {
    Decoder.decodeBooleanLiteral(literalId)
  }

  def handleStringLiteralLookup(store: KeyValueStore, collectionId: Long, literalId: Long): StringLiteral = {
    val res = store.get(Encoder.encodeIdToStringKey(collectionId, literalId))
    StringLiteral(Decoder.decodeStringLiteral(res.get))
  }

  def handleLangLiteralLookup(store: KeyValueStore, collectionId: Long, literalId: Long): LangLiteral = {
    //TODO lookup in IdToLangLiteral
    ???
  }

  def fetchNamedEntityId(store: KeyValueStore, collectionId: Long, entity: NamedEntity): Option[Long] = {
    val res = store.get(Encoder.encodeNamedEntitiesToIdKey(collectionId, entity))
    if (res.nonEmpty) {
      Some(res.get.toLong())
    } else {
      None
    }
  }

  def fetchAnonymousEntityId(store: KeyValueStore, collectionId: Long, entity: AnonymousEntity): Option[Long] = {
    val res = store.get(Encoder.encodeAnonymousEntityKey(collectionId, entity.identifier))
    if (res.nonEmpty) {
      Some(entity.identifier)
    } else {
      None
    }
  }

  def fetchPredicateId(store: KeyValueStore, collectionId: Long, predicate: Predicate): Option[Long] = {
    val res = store.get(Encoder.encodePredicatesToIdKey(collectionId, predicate))
    if (res.nonEmpty) {
      Some(res.get.toLong())
    } else {
      None
    }
  }

  def fetchLangLiteralId(store: KeyValueStore, collectionId: Long, langLiteral: LangLiteral): Option[Long] = {
    //TODO look up in LangLiteralToId
    //TODO return accordingly
    ???
  }

  def fetchStringLiteralId(store: KeyValueStore, collectionId: Long, stringLiteral: StringLiteral): Option[Long] = {
    val res = store.get(Encoder.encodeStringToIdKey(collectionId, stringLiteral))
    if (res.nonEmpty) {
      Some(res.get.toLong())
    } else {
      None
    }
  }

  def lookupSubject(store: KeyValueStore, collectionId: Long, subject: Option[Entity]): Option[ObjectEncoding] = {
    subject flatMap {
      case n: NamedEntity => {
        fetchNamedEntityId(store, collectionId, n) flatMap { i =>
          Some(ObjectEncoding(TypeCodes.NamedEntity, i))
        }
      }
      case a: AnonymousEntity => {
        fetchAnonymousEntityId(store, collectionId, a) flatMap { id: Long =>
          Some(ObjectEncoding(TypeCodes.AnonymousEntity, id))
        }
      }
    }
  }

  def lookupPredicate(store: KeyValueStore, collectionId: Long, predicate: Option[Predicate]): Option[Long] = {
    predicate flatMap {
      fetchPredicateId(store, collectionId, _) flatMap { Some(_) }
    }
  }

  def lookupObject(store: KeyValueStore, collectionId: Long, `object`: Option[Object]): Option[ObjectEncoding] = {
    `object` flatMap {
      case n: NamedEntity => fetchNamedEntityId(store, collectionId, n) flatMap { id =>
        Some(ObjectEncoding(TypeCodes.NamedEntity, id))
      }
      case a: AnonymousEntity => fetchAnonymousEntityId(store, collectionId, a) flatMap { id =>
        Some(ObjectEncoding(TypeCodes.AnonymousEntity, id))
      }
      case l: LangLiteral => fetchLangLiteralId(store, collectionId, l) flatMap { id =>
        Some(ObjectEncoding(TypeCodes.LangLiteral, id))
      }
      case s: StringLiteral => fetchStringLiteralId(store, collectionId, s) flatMap { id =>
        Some(ObjectEncoding(TypeCodes.String, id))
      }
      case d: DoubleLiteral => ???
      case b: BooleanLiteral => ???
      case l: LongLiteral => Some(ObjectEncoding(TypeCodes.Long, l.value))
    }
  }

  def matchStatementsImpl(store: KeyValueStore,
                          collectionName: NamedEntity,
                          subject: Option[Entity] = None,
                          predicate: Option[Predicate] = None,
                          `object`: Option[Object] = None): Iterable[PersistedStatement] = {
    val collectionId = fetchCollectionId(store, collectionName)
    if (collectionId.nonEmpty) {
      val luSubject = lookupSubject(store, collectionId.get, subject)
      val luPredicate = lookupPredicate(store, collectionId.get, predicate)
      val luObject = lookupObject(store, collectionId.get, `object`)

      if ((subject.nonEmpty && luSubject.isEmpty) ||
          (predicate.nonEmpty && luPredicate.isEmpty) ||
          (`object`.nonEmpty && luObject.isEmpty)) {
        return Iterable.empty
      }

      if (subject.nonEmpty) {
        if (predicate.nonEmpty) {
          matchStatementsSPO(store, collectionName, collectionId.get, luSubject, luPredicate, luObject)
        } else {
          matchStatementsSOP(store, collectionName, collectionId.get, luSubject, luPredicate, luObject)
        }
      } else if (predicate.nonEmpty) {
        if (`object`.nonEmpty) {
          matchStatementsPOS(store, collectionName, collectionId.get, luSubject, luPredicate, luObject)
        } else {
          matchStatementsPSO(store, collectionName, collectionId.get, luSubject, luPredicate, luObject)
        }
      } else if (`object`.nonEmpty) {
        if (subject.nonEmpty) {
          matchStatementsOSP(store, collectionName, collectionId.get, luSubject, luPredicate, luObject)
        } else {
          matchStatementsOPS(store, collectionName, collectionId.get, luSubject, luPredicate, luObject)
        }
      } else {
        val res = readAllStatements(store, collectionName) //TODO this causes the collection id to be looked up twice
        if (res.nonEmpty) {
          res.get
        } else {
          Iterable.empty
        }
      }
    } else {
      Iterable.empty
    }
  }

//  def matchStatementsImpl(store: KeyValueStore,
//                          collectionName: NamedEntity,
//                          subject: Option[Entity],
//                          predicate: Option[Predicate],
//                          literalRange: Range[_, _]): Iterable[PersistedStatement] = {
//    val collectionId = fetchCollectionId(store, collectionName)
//    if (collectionId.nonEmpty) {
//      val luSubject = lookupSubject(store, collectionId.get, subject)
//      val luPredicate = lookupPredicate(store, collectionId.get, predicate)
//      val luObjectStart = lookupObject(store, collectionId.get, literalRange.start)
//      val luObjectEnd = lookupObject(store, collectionId.get, literalRange.`end`)
//      //TODO look up object and probably convert Range[_, _] to (ByteVector, ByteVector)
//
//      if ((subject.nonEmpty && luSubject.isEmpty) ||
//        (predicate.nonEmpty && luPredicate.isEmpty)) {
//        return Iterable.empty
//      }
//
//      if (subject.nonEmpty) {
//        matchStatementsSOP(store, collectionName, collectionId.get, luSubject, luPredicate, literalRange)
//      } else if (predicate.nonEmpty) {
//        matchStatementsPOS(store, collectionName, collectionId.get, luSubject, luPredicate, literalRange)
//      } else {
//        if (subject.nonEmpty) {
//          matchStatementsOSP(store, collectionName, collectionId.get, luSubject, luPredicate, literalRange)
//        } else {
//          matchStatementsOPS(store, collectionName, collectionId.get, luSubject, luPredicate, literalRange)
//        }
//      }
//    } else {
//      Iterable.empty
//    }
//  }

  def matchStatementsSPO(store: KeyValueStore,
                         collectionName: NamedEntity,
                         collectionId: Long,
                         subject: Option[ObjectEncoding],
                         predicate: Option[Long],
                         `object`: Option[ObjectEncoding]): Iterable[PersistedStatement] = {
    val prefixPattern = Encoder.encodeSPOPrefix(collectionId, subject, predicate, `object`)
    store.prefix(prefixPattern).map { bv =>
      val res = Decoder.decodeSPOC(bv._1).get
      spocToPersistedStatement(store, collectionName, collectionId, res)
    }
  }

  def matchStatementsSOP(store: KeyValueStore,
                         collectionName: NamedEntity,
                         collectionId: Long,
                         subject: Option[ObjectEncoding],
                         predicate: Option[Long],
                         `object`: Option[ObjectEncoding]): Iterable[PersistedStatement] = {
    val prefixPattern = Encoder.encodeSOPPrefix(collectionId, subject, predicate, `object`)
    store.prefix(prefixPattern).map { bv =>
      val res = Decoder.decodeSOPC(bv._1).get
      spocToPersistedStatement(store, collectionName, collectionId, res)
    }
  }

  def matchStatementsPSO(store: KeyValueStore,
                         collectionName: NamedEntity,
                         collectionId: Long,
                         subject: Option[ObjectEncoding],
                         predicate: Option[Long],
                         `object`: Option[ObjectEncoding]): Iterable[PersistedStatement] = {
    val prefixPattern = Encoder.encodePSOPrefix(collectionId, subject, predicate, `object`)
    store.prefix(prefixPattern).map { bv =>
      val res = Decoder.decodePSOC(bv._1).get
      spocToPersistedStatement(store, collectionName, collectionId, res)
    }
  }

  def matchStatementsPOS(store: KeyValueStore,
                         collectionName: NamedEntity,
                         collectionId: Long,
                         subject: Option[ObjectEncoding],
                         predicate: Option[Long],
                         `object`: Option[ObjectEncoding]): Iterable[PersistedStatement] = {
    val prefixPattern = Encoder.encodePOSPrefix(collectionId, subject, predicate, `object`)
    store.prefix(prefixPattern).map { bv =>
      val res = Decoder.decodePOSC(bv._1).get
      spocToPersistedStatement(store, collectionName, collectionId, res)
    }
  }

  def matchStatementsOSP(store: KeyValueStore,
                         collectionName: NamedEntity,
                         collectionId: Long,
                         subject: Option[ObjectEncoding],
                         predicate: Option[Long],
                         `object`: Option[ObjectEncoding]): Iterable[PersistedStatement] = {
    val prefixPattern = Encoder.encodeOSPPrefix(collectionId, subject, predicate, `object`)
    store.prefix(prefixPattern).map { bv =>
      val res = Decoder.decodeOSPC(bv._1).get
      spocToPersistedStatement(store, collectionName, collectionId, res)
    }
  }

  def matchStatementsOPS(store: KeyValueStore,
                         collectionName: NamedEntity,
                         collectionId: Long,
                         subject: Option[ObjectEncoding],
                         predicate: Option[Long],
                         `object`: Option[ObjectEncoding]): Iterable[PersistedStatement] = {
    val prefixPattern = Encoder.encodeOPSPrefix(collectionId, subject, predicate, `object`)
    store.prefix(prefixPattern).map { bv =>
      val res = Decoder.decodeOPSC(bv._1).get
      spocToPersistedStatement(store, collectionName, collectionId, res)
    }
  }

//  def matchStatementsSOP(store: KeyValueStore,
//                         collectionName: NamedEntity,
//                         collectionId: Long,
//                         subject: Option[ObjectEncoding],
//                         predicate: Option[Long],
//                         literalRange: Range[_, _]): Iterable[PersistedStatement] = {
//    val startStopPattern = Encoder.encodeSOPStartStop(collectionId, subject, predicate, literalRange)
//    store.range(startStopPattern._1, startStopPattern._2).map { bv =>
//      val res = Decoder.decodeSOPC(bv._1).get
//      spocToPersistedStatement(store, collectionName, collectionId, res)
//    }
//  }
//
//  def matchStatementsPOS(store: KeyValueStore,
//                         collectionName: NamedEntity,
//                         collectionId: Long,
//                         subject: Option[ObjectEncoding],
//                         predicate: Option[Long],
//                         literalRange: Range[_, _]): Iterable[PersistedStatement] = {
//    val startStopPattern = Encoder.encodePOSStartStop(collectionId, subject, predicate, literalRange)
//    store.range(startStopPattern._1, startStopPattern._2).map { bv =>
//      val res = Decoder.decodePOSC(bv._1).get
//      spocToPersistedStatement(store, collectionName, collectionId, res)
//    }
//  }
//
//  def matchStatementsOSP(store: KeyValueStore,
//                         collectionName: NamedEntity,
//                         collectionId: Long,
//                         subject: Option[ObjectEncoding],
//                         predicate: Option[Long],
//                         literalRange: Range[_, _]): Iterable[PersistedStatement] = {
//    val startStopPattern = Encoder.encodeOSPStartStop(collectionId, subject, predicate, literalRange)
//    store.range(startStopPattern._1, startStopPattern._2).map { bv =>
//      val res = Decoder.decodeOSPC(bv._1).get
//      spocToPersistedStatement(store, collectionName, collectionId, res)
//    }
//  }
//
//  def matchStatementsOPS(store: KeyValueStore,
//                         collectionName: NamedEntity,
//                         collectionId: Long,
//                         subject: Option[ObjectEncoding],
//                         predicate: Option[Long],
//                         literalRange: Range[_, _]): Iterable[PersistedStatement] = {
//    val startStopPattern = Encoder.encodeOPSStartStop(collectionId, subject, predicate, literalRange)
//    store.range(startStopPattern._1, startStopPattern._2).map { bv =>
//      val res = Decoder.decodeOPSC(bv._1).get
//      spocToPersistedStatement(store, collectionName, collectionId, res)
//    }
//  }

  def statementByContextImpl(store: KeyValueStore,
                             collectionName: NamedEntity,
                             context: AnonymousEntity): Option[PersistedStatement] = {
    ???
  }
}
