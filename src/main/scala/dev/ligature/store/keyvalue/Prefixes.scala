/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

private object Prefixes {
  val CollectionNameToId: Byte = 0.toByte
  val IdToCollectionName: Byte = 1.toByte
  val CollectionNameCounter: Byte = 2.toByte
  val SEDC: Byte = 3.toByte
  val SDEC: Byte = 4.toByte
  val ESDC: Byte = 5.toByte
  val EDSC: Byte = 6.toByte
  val DSEC: Byte = 7.toByte
  val DESC: Byte = 8.toByte
  val CSED: Byte = 9.toByte
  val CollectionCounter: Byte = 10.toByte
  val NodeToId: Byte = 11.toByte
  val IdToNode: Byte = 12.toByte
  val AnonymousNode: Byte = 13.toByte
  val LangLiteralToId: Byte = 14.toByte
  val IdToLangLiteral: Byte = 15.toByte
  val StringToId: Byte = 16.toByte
  val IdToString: Byte = 17.toByte
}

private object TypeCodes {
  val Node: Byte = 0.toByte
  val AnonymousNode: Byte = 1.toByte
  val LangLiteral: Byte = 2.toByte
  val String: Byte = 3.toByte
  val Boolean: Byte = 4.toByte
  val Long: Byte = 5.toByte
  val Double: Byte = 6.toByte
}
