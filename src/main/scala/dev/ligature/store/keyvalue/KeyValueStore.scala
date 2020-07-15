/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import cats.effect.{IO, Resource}
import dev.ligature.{ReadTx, WriteTx}
import scodec.bits.ByteVector

import scala.util.Try

/**
 * A trait that handles lower level access for working with LigatureStores.
 */
trait KeyValueStore {
  def put(key: ByteVector, value: ByteVector): Try[(ByteVector, ByteVector)]
  def delete(key: ByteVector): Try[ByteVector]
  def scan(start: ByteVector, end: ByteVector): Iterable[(ByteVector, ByteVector)]
  def close()
  def isOpen: Boolean
  def write: Resource[IO, WriteTx]
  def compute: Resource[IO, ReadTx]
}
