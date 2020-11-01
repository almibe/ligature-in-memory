/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.codec

private object Prefixes {
  val CollectionNameToId: Byte = 0.toByte
  val IdToCollectionName: Byte = 1.toByte
  val CollectionNameCounter: Byte = 2.toByte
  val SPOC: Byte = 3.toByte
  val SOPC: Byte = 4.toByte
  val PSOC: Byte = 5.toByte
  val POSC: Byte = 6.toByte
  val OSPC: Byte = 7.toByte
  val OPSC: Byte = 8.toByte
  val CSPO: Byte = 9.toByte
  val CollectionCounter: Byte = 10.toByte
  val NamedNodesToId: Byte = 11.toByte
  val IdToNamedNodes: Byte = 12.toByte
  val AnonymousElements: Byte = 13.toByte
  val LangLiteralToId: Byte = 14.toByte
  val IdToLangLiteral: Byte = 15.toByte
  val StringToId: Byte = 16.toByte
  val IdToString: Byte = 17.toByte
}

private object TypeCodes {
  val NamedNode: Byte = 0.toByte
  val AnonymousElement: Byte = 1.toByte
  val LangLiteral: Byte = 2.toByte
  val String: Byte = 3.toByte
  val Boolean: Byte = 4.toByte
  val Long: Byte = 5.toByte
  val Double: Byte = 6.toByte
}
