/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.operations

import dev.ligature.store.keyvalue.codec.Encoder.ElementEncoding
import dev.ligature.{AnonymousElement, BooleanLiteral, DoubleLiteral, Element, LangLiteral, LongLiteral, NamedElement, StringLiteral}
import dev.ligature.store.keyvalue.{KeyValueStore, TypeCodes}

object ObjectOperations {
  def handleObjectLookup(store: KeyValueStore, collectionId: Long, objectType: Byte, objectId: Long): Element = {
    objectType match {
      case TypeCodes.NamedElement => handleNamedElementLookup(store, collectionId, objectId)
      case TypeCodes.AnonymousElement => AnonymousElement(objectId)
      case TypeCodes.Double => decodeDoubleLiteral(objectId)
      case TypeCodes.Long => LongLiteral(objectId)
      case TypeCodes.Boolean => decodeBooleanLiteral(objectId)
      case TypeCodes.String => handleStringLiteralLookup(store, collectionId, objectId)
      case TypeCodes.LangLiteral => handleLangLiteralLookup(store, collectionId, objectId)
      case _ => throw new RuntimeException(s"Illegal object type $objectType")
    }
  }

  def lookupObject(store: KeyValueStore, collectionId: Long, `object`: Option[Element]): Option[ElementEncoding] = {
    `object` flatMap {
      case n: NamedElement => fetchNamedElementId(store, collectionId, n) flatMap { id =>
        Some(ElementEncoding(TypeCodes.NamedElement, id))
      }
      case a: AnonymousElement => fetchAnonymousElementId(store, collectionId, a) flatMap { id =>
        Some(ElementEncoding(TypeCodes.AnonymousElement, id))
      }
      case l: LangLiteral => fetchLangLiteralId(store, collectionId, l) flatMap { id =>
        Some(ElementEncoding(TypeCodes.LangLiteral, id))
      }
      case s: StringLiteral => fetchStringLiteralId(store, collectionId, s) flatMap { id =>
        Some(ElementEncoding(TypeCodes.String, id))
      }
      case d: DoubleLiteral => ???
      case b: BooleanLiteral => ???
      case l: LongLiteral => Some(ElementEncoding(TypeCodes.Long, l.value))
    }
  }

}
