package dev.ligature.store.keyvalue

import cats.effect.IO
import dev.ligature.{AnonymousEntity, DoubleLiteral, DoubleLiteralRange, Entity, LangLiteral, LangLiteralRange, LongLiteral, LongLiteralRange, NamedEntity, Object, PersistedStatement, Predicate, Range, StringLiteral, StringLiteralRange}
import scodec.bits.ByteVector

import scala.util.Try

object Common {
  def collections(store: KeyValueStore): IO[Iterable[NamedEntity]] = {
    IO {
      val collectionNameToId = store.scan(ByteVector.fromByte(Prefixes.CollectionNameToId),
        ByteVector.fromInt(Prefixes.CollectionNameToId + 1.toByte))
      collectionNameToId.map { encoded =>
        encoded._1.drop(1).decodeUtf8.map(NamedEntity).getOrElse(throw new RuntimeException("Invalid Name"))
      }
    }
  }

  def createCollection(store: KeyValueStore): IO[Try[NamedEntity]] = {
    if (active.get()) {
      if (!workingState.get().contains(collection)) {
        val oldState = workingState.get()
        val newState = oldState.updated(collection,
          CollectionValue(new AtomicReference(new HashSet[PersistedStatement]()),
            new AtomicLong(0)))
        val result = workingState.compareAndSet(oldState, newState)
        IO { if (result) Success(collection) else Failure(new RuntimeException("Couldn't persist new collection.")) }
      } else {
        IO { Success(collection) } //collection exists
      }
    } else {
      throw new RuntimeException("Transaction is closed.")
    }
  }

  def collectionExists(store: KeyValueStore, collectionName: NamedEntity): Boolean = {
    ???
  }

  def readAllStatements(store: KeyValueStore, collectionName: NamedEntity): Iterable[PersistedStatement] = {
    ???
  }

  def collectionId(store: KeyValueStore, collectionName: NamedEntity): Long = {
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
