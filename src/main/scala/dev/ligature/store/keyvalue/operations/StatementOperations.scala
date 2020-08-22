/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.operations

import dev.ligature.{AnonymousElement, Element, NamedElement, PersistedStatement, Statement, Subject}
import dev.ligature.store.keyvalue.KeyValueStore
import dev.ligature.store.keyvalue.codec.Encoder
import dev.ligature.store.keyvalue.codec.Encoder.ElementEncoding
import scodec.bits.ByteVector

object StatementOperations {
  def readAllStatements(store: KeyValueStore, collectionName: NamedElement): Option[Iterable[PersistedStatement]] = {
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

  def spocToPersistedStatement(store: KeyValueStore, collectionName: NamedElement, collectionId: Long, spoc: Encoder.SPOC): PersistedStatement = {
    val subject = handleSubjectLookup(store, collectionId, spoc.subject.`type`, spoc.subject.id)
    val predicate = handlePredicateLookup(store, collectionId, spoc.predicateId)
    val obj = handleObjectLookup(store, collectionId, spoc.`object`.`type`, spoc.`object`.id)
    val context = AnonymousElement(spoc.context)
    val statement = Statement(subject, predicate, obj)
    PersistedStatement(collectionName, statement, context)
  }

  def matchStatementsImpl(store: KeyValueStore,
                          collectionName: NamedElement,
                          subject: Option[Subject] = None,
                          predicate: Option[Predicate] = None,
                          `object`: Option[Element] = None): Iterable[PersistedStatement] = {
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
  //                          collectionName: NamedElement,
  //                          subject: Option[Subject],
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
                         collectionName: NamedElement,
                         collectionId: Long,
                         subject: Option[ElementEncoding],
                         predicate: Option[Long],
                         `object`: Option[ElementEncoding]): Iterable[PersistedStatement] = {
    val prefixPattern = Encoder.encodeSPOPrefix(collectionId, subject, predicate, `object`)
    store.prefix(prefixPattern).map { bv =>
      val res = Decoder.decodeSPOC(bv._1).get
      spocToPersistedStatement(store, collectionName, collectionId, res)
    }
  }

  def matchStatementsSOP(store: KeyValueStore,
                         collectionName: NamedElement,
                         collectionId: Long,
                         subject: Option[ElementEncoding],
                         predicate: Option[Long],
                         `object`: Option[ElementEncoding]): Iterable[PersistedStatement] = {
    val prefixPattern = Encoder.encodeSOPPrefix(collectionId, subject, predicate, `object`)
    store.prefix(prefixPattern).map { bv =>
      val res = Decoder.decodeSOPC(bv._1).get
      spocToPersistedStatement(store, collectionName, collectionId, res)
    }
  }

  def matchStatementsPSO(store: KeyValueStore,
                         collectionName: NamedElement,
                         collectionId: Long,
                         subject: Option[ElementEncoding],
                         predicate: Option[Long],
                         `object`: Option[ElementEncoding]): Iterable[PersistedStatement] = {
    val prefixPattern = Encoder.encodePSOPrefix(collectionId, subject, predicate, `object`)
    store.prefix(prefixPattern).map { bv =>
      val res = Decoder.decodePSOC(bv._1).get
      spocToPersistedStatement(store, collectionName, collectionId, res)
    }
  }

  def matchStatementsPOS(store: KeyValueStore,
                         collectionName: NamedElement,
                         collectionId: Long,
                         subject: Option[ElementEncoding],
                         predicate: Option[Long],
                         `object`: Option[ElementEncoding]): Iterable[PersistedStatement] = {
    val prefixPattern = Encoder.encodePOSPrefix(collectionId, subject, predicate, `object`)
    store.prefix(prefixPattern).map { bv =>
      val res = Decoder.decodePOSC(bv._1).get
      spocToPersistedStatement(store, collectionName, collectionId, res)
    }
  }

  def matchStatementsOSP(store: KeyValueStore,
                         collectionName: NamedElement,
                         collectionId: Long,
                         subject: Option[ElementEncoding],
                         predicate: Option[Long],
                         `object`: Option[ElementEncoding]): Iterable[PersistedStatement] = {
    val prefixPattern = Encoder.encodeOSPPrefix(collectionId, subject, predicate, `object`)
    store.prefix(prefixPattern).map { bv =>
      val res = Decoder.decodeOSPC(bv._1).get
      spocToPersistedStatement(store, collectionName, collectionId, res)
    }
  }

  def matchStatementsOPS(store: KeyValueStore,
                         collectionName: NamedElement,
                         collectionId: Long,
                         subject: Option[ElementEncoding],
                         predicate: Option[Long],
                         `object`: Option[ElementEncoding]): Iterable[PersistedStatement] = {
    val prefixPattern = Encoder.encodeOPSPrefix(collectionId, subject, predicate, `object`)
    store.prefix(prefixPattern).map { bv =>
      val res = Decoder.decodeOPSC(bv._1).get
      spocToPersistedStatement(store, collectionName, collectionId, res)
    }
  }

  //  def matchStatementsSOP(store: KeyValueStore,
  //                         collectionName: NamedElement,
  //                         collectionId: Long,
  //                         subject: Option[ElementEncoding],
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
  //                         collectionName: NamedElement,
  //                         collectionId: Long,
  //                         subject: Option[ElementEncoding],
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
  //                         collectionName: NamedElement,
  //                         collectionId: Long,
  //                         subject: Option[ElementEncoding],
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
  //                         collectionName: NamedElement,
  //                         collectionId: Long,
  //                         subject: Option[ElementEncoding],
  //                         predicate: Option[Long],
  //                         literalRange: Range[_, _]): Iterable[PersistedStatement] = {
  //    val startStopPattern = Encoder.encodeOPSStartStop(collectionId, subject, predicate, literalRange)
  //    store.range(startStopPattern._1, startStopPattern._2).map { bv =>
  //      val res = Decoder.decodeOPSC(bv._1).get
  //      spocToPersistedStatement(store, collectionName, collectionId, res)
  //    }
  //  }

  def statementByContextImpl(store: KeyValueStore,
                             collectionName: NamedElement,
                             context: AnonymousElement): Option[PersistedStatement] = {
    ???
  }

}
