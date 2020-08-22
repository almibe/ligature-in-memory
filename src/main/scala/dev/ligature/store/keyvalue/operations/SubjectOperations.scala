/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.operations

import dev.ligature.store.keyvalue.codec.Encoder.ElementEncoding
import dev.ligature.{AnonymousElement, NamedElement, Subject}
import dev.ligature.store.keyvalue.{KeyValueStore, TypeCodes} .handleNamedElementLookup

object SubjectOperations {
  def handleSubjectLookup(store: KeyValueStore, collectionId: Long, subjectType: Byte, subjectId: Long): Subject = {
    subjectType match {
      case TypeCodes.NamedElement => handleNamedElementLookup(store, collectionId, subjectId)
      case TypeCodes.AnonymousElement => AnonymousElement(subjectId)
      case _ => throw new RuntimeException(s"Illegal subject type $subjectType")
    }
  }

  def lookupSubject(store: KeyValueStore, collectionId: Long, subject: Option[Subject]): Option[ElementEncoding] = {
    subject flatMap {
      case n: NamedElement => {
        fetchNamedElementId(store, collectionId, n) flatMap { i =>
          Some(ElementEncoding(TypeCodes.NamedElement, i))
        }
      }
      case a: AnonymousElement => {
        fetchAnonymousElementId(store, collectionId, a) flatMap { id: Long =>
          Some(ElementEncoding(TypeCodes.AnonymousElement, id))
        }
      }
    }
  }

  def subjectType(element: Entity): Byte =
    entity match {
      case _: NamedElement => TypeCodes.NamedElement
      case _: AnonymousElement => TypeCodes.AnonymousElement
    }

  private def fetchOrCreateSubject(store: KeyValueStore, collectionId: Long, subject: Entity): (Entity, Long) = {
    subject match {
      case a: AnonymousElement => fetchOrCreateAnonymousElement(store, collectionId, a)
      case n: NamedElement => fetchOrCreateNamedElement(store, collectionId, n)
      case c: Context => fetchOrCreateContext(store, collectionId, c)
    }
  }


}
