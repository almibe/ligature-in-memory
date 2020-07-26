/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import scodec.bits.ByteVector

import scala.util.Try

/**
 * A trait that handles lower level access for working with LigatureStores.
 */
trait KeyValueStore {
  def get(key: ByteVector): Option[ByteVector]
  def put(key: ByteVector, value: ByteVector): Try[(ByteVector, ByteVector)]
  def delete(key: ByteVector): Try[ByteVector]
  def prefix(prefix: ByteVector): Iterable[(ByteVector, ByteVector)]
  def range(start: ByteVector, end: ByteVector): Iterable[(ByteVector, ByteVector)]
}
