/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.common

object Prefixes {
  val CollectionNameToId = 0.toByte
  val IdToCollectionName = 1.toByte
  val CollectionNameCounter = 2.toByte
  val SPOC = 3.toByte
  val SOPC = 4.toByte
  val PSOC = 5.toByte
  val POSC = 6.toByte
  val OSPC = 7.toByte
  val OPSC = 8.toByte
  val SCPO = 9.toByte
  val CollectionCounter = 10.toByte
  val NamedEntitiesToId = 11.toByte
  val IdToNamedEntities = 12.toByte
  val AnonymousEntities = 13.toByte
  val PredicatesToId = 14.toByte
  val IdToPredicates = 15.toByte
  val LangLiteralToId = 16.toByte
  val IdToLangLiteral = 17.toByte
  val StringToId = 18.toByte
  val IdToString = 19.toByte
}

object TypeCodes {
  val NamedEntity = 0.toByte
  val AnonymousEntity = 1.toByte
  val LangLiteral = 2.toByte
  val String = 3.toByte
  val Boolean = 4.toByte
  val Long = 5.toByte
  val Double = 6.toByte
}
