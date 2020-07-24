/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import dev.ligature.{AnonymousEntity, BooleanLiteral, DoubleLiteral, DoubleLiteralRange, Entity, LangLiteral, LangLiteralRange, Literal, LongLiteral, LongLiteralRange, NamedEntity, Object, PersistedStatement, Predicate, Range, Statement, StringLiteral, StringLiteralRange}

import scala.util.{Success, Try}

object KeyValueOperations {
  def collections(store: KeyValueStore): Iterable[NamedEntity] = {
    val collectionNameToId = store.scan(ByteVector.fromByte(Prefixes.CollectionNameToId),
      ByteVector.fromByte((Prefixes.CollectionNameToId + 1.toByte).toByte))
    collectionNameToId.map { encoded =>
      encoded._1.drop(1).decodeUtf8.map(NamedEntity).getOrElse(throw new RuntimeException("Invalid Name"))
    }
  }

  def createCollection(store: KeyValueStore, collection: NamedEntity): Try[Long] = {
    val id = fetchCollectionId(store, collection)
    if (id.isEmpty) {
      val nextId = nextCollectionNameId(store)
      val collectionNameToIdEncoder = byte ~ utf8
      val idToCollectionNameEncoder = byte ~ long(64)
      val collectionNameToIdEncodedKey = collectionNameToIdEncoder.encode((Prefixes.CollectionNameToId, collection.identifier)).require.bytes
      val idToCollectionNameEncodedKey = idToCollectionNameEncoder.encode((Prefixes.IdToCollectionName, nextId)).require.bytes
      val idByteVector = long(64).encode(nextId).require.bytes
      store.put(collectionNameToIdEncodedKey, idByteVector)
      store.put(idToCollectionNameEncodedKey, utf8.encode(collection.identifier).require.bytes)
      Success(idByteVector)
    } else {
      Success(id.get)
    }
  }

  def deleteCollection(store: KeyValueStore, collection: NamedEntity): Try[NamedEntity] = {
    val id = fetchCollectionId(store, collection).orNull
    if (id != null) {
      store.delete(byteString.encode(Prefixes.CollectionNameToId, collection.identifier).require.bytes)
      store.delete(byteBytes.encode(Prefixes.IdToCollectionName, id).require.bytes)
      Range(Prefixes.SPOC, Prefixes.IdToString + 1).foreach { prefix =>
        store.delete(byteBytes.encode(prefix.toByte, id).require.bytes)
      }
      Success(collection)
    } else {
      Success(collection)
    }
  }

  def nextCollectionNameId(store: KeyValueStore): Long = {
    val currentId = store.get(byte.encode(Prefixes.CollectionNameCounter).require.bytes)
    currentId match {
      case Some(bv) => {
        val nextId = bv.toLong() + 1
        store.put(byte.encode(Prefixes.CollectionNameCounter).require.bytes, long(64).encode(nextId).require.bytes)
        nextId
      }
      case None => {
        val nextId = 0
        store.put(byte.encode(Prefixes.CollectionNameCounter).require.bytes, long(64).encode(nextId).require.bytes)
        nextId
      }
    }
  }

  def fetchCollectionId(store: KeyValueStore, collectionName: NamedEntity): Option[Long] = {
    val encoder = byte ~ utf8
    val encoded = encoder.encode(Prefixes.CollectionNameToId, collectionName.identifier).require.bytes
    store.get(encoded)
  }

  def fetchOrCreateCollection(store: KeyValueStore, collectionName: NamedEntity): Long = {
    val id = fetchCollectionId(store, collectionName)
    if (id.isEmpty) {
      createCollection(store, collectionName).get
    } else {
      id.get
    }
  }

  def readAllStatements(store: KeyValueStore, collectionName: NamedEntity): Option[Iterable[PersistedStatement]] = {
    val collectionId = fetchCollectionId(store, collectionName)
    if (collectionId.nonEmpty) {
      val itr = store.scan(byteBytes.encode(Prefixes.SPOC, collectionId.get).require.bytes,
        byteBytes.encode(Prefixes.SPOC, collectionId.get).require.bytes)
      Some(itr.map { entry: (ByteVector, ByteVector) =>
        val attempt = spoc.decode(entry._1.bits)
        if (attempt.isSuccessful) {
          val (_, _, sType, eSubject, ePredicate, oType, eObj, eContext) = attempt.require.value
          val subject = handleSubjectLookup(store, collectionId.get, sType, eSubject)
          val predicate = handlePredicateLookup(store, collectionId.get, ePredicate)
          val obj = handleObjectLookup(store, collectionId.get, oType, eObj)
          val context = handleContextLookup(store, collectionId.get, eContext)
          val statement = Statement(subject, predicate, obj)
          PersistedStatement(collectionName, statement, context)
        } else {
          ???
        }
      })
    } else {
      None
    }
  }

  def handleSubjectLookup(store: KeyValueStore, collectionId: Long, subjectType: Byte, subjectId: Long): Entity = {
    ???
  }

  def handlePredicateLookup(store: KeyValueStore, collectionId: Long, predicateId: Long): Predicate = {
    ???
  }

  def handleObjectLookup(store: KeyValueStore, collectionId: Long, objectType: Byte, objectValue: Long): Object = {
    ???
  }

  def handleContextLookup(store: KeyValueStore, collectionId: Long, context: Long): AnonymousEntity = {
    ???
  }

  /**
   * Creates an new AnonymousEntity for the given collection name.
   * Returns a tuple of the id for the new entity as a ByteVector and the actual AnonymousEntity.
   */
  def newEntity(store: KeyValueStore, collectionName: NamedEntity): AnonymousEntity = {
    val id = fetchOrCreateCollection(store, collectionName)
    val key = byteBytes.encode(Prefixes.CollectionCounter, id).require.bytes
    val collectionCounter = store.get(key)
    val counterValue = if (collectionCounter.nonEmpty) {
      long(64).decode(collectionCounter.get.bits).require.value + 1L
    } else {
      0
    }
    store.put(key, long(64).encode(counterValue).require.bytes)
    AnonymousEntity(counterValue)
    store.put(byteBytesLong.encode(Prefixes.AnonymousEntities, id, counterValue).require.bytes, ByteVector.empty)
    (long(64).encode(counterValue).require.bytes, AnonymousEntity(counterValue))
  }

  def addStatement(store: KeyValueStore,
                   collectionName: NamedEntity,
                   statement: Statement): Try[PersistedStatement] = {
    //TODO check if statement already exists
    var id = fetchCollectionId(store, collectionName).orNull
    if (id == null) {
      id = createCollection(store, collectionName).get
    }
    val context = newEntity(store, collectionName)
    val subject = fetchOrCreateSubject(store, collectionName, statement.subject)
    val predicate = fetchOrCreatePredicate(store, collectionName, statement.predicate)
    val obj = fetchOrCreateObject(store, collectionName, statement.`object`)
    store.put(spoc.encode((Prefixes.SPOC, id)).require.bytes, ByteVector.empty)
    //TODO store.put(sopc.encode(???).require.bytes, ByteVector.empty)
    //TODO store.put(psoc.encode(???).require.bytes, ByteVector.empty)
    //TODO store.put(posc.encode(???).require.bytes, ByteVector.empty)
    //TODO store.put(ospc.encode(???).require.bytes, ByteVector.empty)
    //TODO store.put(opsc.encode(???).require.bytes, ByteVector.empty)
    //TODO store.put(cspo.encode(???).require.bytes, ByteVector.empty)
    ???
//      persistedStatement <- IO { PersistedStatement(collection, statement, context.get) }
//      statements <- IO { workingState.get()(collection).statements }
//      _ <- IO { statements.set(statements.get().incl(persistedStatement)) }
  }

  def fetchOrCreateSubject(store: KeyValueStore, collectionName: NamedEntity, subject: Entity): Long = {
    subject match {
      case a: AnonymousEntity => fetchOrCreateAnonymousEntity(store, collectionName, a)
      case n: NamedEntity => fetchOrCreateNamedEntity(store, collectionName, n)
    }
  }

  def fetchOrCreatePredicate(store: KeyValueStore, collectionName: NamedEntity, predicate: Predicate): Long = {
    ???
  }

  def fetchOrCreateObject(store: KeyValueStore, collectionName: NamedEntity, value: Object): Long = {
    value match {
      case a: AnonymousEntity => fetchOrCreateAnonymousEntity(store, collectionName, a)
      case n: NamedEntity => fetchOrCreateNamedEntity(store, collectionName, n)
      case l: LangLiteral => fetchOrCreateLangLiteral(store, collectionName, l)
      case d: DoubleLiteral => fetchOrCreateDoubleLiteral(store, collectionName, d)
      case l: LongLiteral => fetchOrCreateLongLiteral(store, collectionName, l)
      case s: StringLiteral => fetchOrCreateStringLiteral(store, collectionName, s)
      case b: BooleanLiteral => fetchOrCreateBooleanLiteral(store, collectionName, b)
    }
  }

  def fetchOrCreateAnonymousEntity(store: KeyValueStore, collectionName: NamedEntity, entity: AnonymousEntity): Long = {
    ???
  }

  def fetchOrCreateNamedEntity(store: KeyValueStore, collectionName: NamedEntity, entity: NamedEntity): Long = {
    ???
  }

  def fetchOrCreateLangLiteral(store: KeyValueStore, collectionName: NamedEntity, literal: LangLiteral): Long = {
    ???
  }

  def fetchOrCreateDoubleLiteral(store: KeyValueStore, collectionName: NamedEntity, literal: DoubleLiteral): Long = {
    ???
  }

  def fetchOrCreateLongLiteral(store: KeyValueStore, collectionName: NamedEntity, literal: LongLiteral): Long = {
    ???
  }

  def fetchOrCreateStringLiteral(store: KeyValueStore, collectionName: NamedEntity, literal: StringLiteral): Long = {
    ???
  }

  def fetchOrCreateBooleanLiteral(store: KeyValueStore, collectionName: NamedEntity, literal: BooleanLiteral): Long = {
    ???
  }

  def matchStatementsImpl(store: KeyValueStore,
                          collectionName: NamedEntity,
                          subject: Option[Entity] = None,
                          predicate: Option[Predicate] = None,
                          `object`: Option[Object] = None): Iterable[PersistedStatement] = {
    ???
//    statements.filter { statement =>
//      statement.statement.subject match {
//        case _ if subject.isEmpty => true
//        case _ => statement.statement.subject == subject.get
//      }
//    }.filter { statement =>
//      statement.statement.predicate match {
//        case _ if predicate.isEmpty => true
//        case _ => statement.statement.predicate == predicate.get
//      }
//    }.filter { statement =>
//      statement.statement.`object` match {
//        case _ if `object`.isEmpty => true
//        case _ => statement.statement.`object` == `object`.get
//      }
//    }
  }

  def matchStatementsImpl(store: KeyValueStore,
                          collectionName: NamedEntity,
                          subject: Option[Entity],
                          predicate: Option[Predicate],
                          range: Range[_]): Iterable[PersistedStatement] = {
    ???
//    statements.filter { statement =>
//      statement.statement.subject match {
//        case _ if subject.isEmpty => true
//        case _ => statement.statement.subject == subject.get
//      }
//    }.filter { statement =>
//      statement.statement.predicate match {
//        case _ if predicate.isEmpty => true
//        case _ => statement.statement.predicate == predicate.get
//      }
//    }.filter { statement =>
//      val s = statement.statement
//      (range, s.`object`) match {
//        case (r: LangLiteralRange, o: LangLiteral) => matchLangLiteralRange(r, o)
//        case (r: StringLiteralRange, o: StringLiteral) => matchStringLiteralRange(r, o)
//        case (r: LongLiteralRange, o: LongLiteral) => matchLongLiteralRange(r, o)
//        case (r: DoubleLiteralRange, o: DoubleLiteral) => matchDoubleLiteralRange(r, o)
//        case _ => false
//      }
//    }
  }

  private def matchLangLiteralRange(range: LangLiteralRange, literal: LangLiteral): Boolean = {
    literal.langTag == range.start.langTag && range.start.langTag == range.end.langTag &&
      literal.value >= range.start.value && literal.value < range.end.value
  }

  private def matchStringLiteralRange(range: StringLiteralRange, literal: StringLiteral): Boolean = {
    literal.value >= range.start && literal.value < range.end
  }

  private def matchLongLiteralRange(range: LongLiteralRange, literal: LongLiteral): Boolean = {
    literal.value >= range.start && literal.value < range.end
  }

  private def matchDoubleLiteralRange(range: DoubleLiteralRange, literal: DoubleLiteral): Boolean = {
    literal.value >= range.start && literal.value < range.end
  }

  def statementByContextImpl(store: KeyValueStore,
                             collectionName: NamedEntity,
                             context: AnonymousEntity): Option[PersistedStatement] = ???
    //statements.find(_.context == context)
}
