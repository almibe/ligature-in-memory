/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import cats.effect.{IO, Resource}
import dev.ligature.{ReadTx, WriteTx}
import scala.util.Try

/**
 * A trait that handles lower level access for working with LigatureStores.
 */
trait KeyValueStore {
  abstract def put(key: Array[Byte], value: Array[Byte]): Try[(Array[Byte], Array[Byte])]
  abstract def delete(key: Array[Byte]): Try[Array[Byte]]
  abstract def scan(start: Array[Byte], end: Array[Byte]): Iterable[(Array[Byte], Array[Byte])]
  abstract def close()
  abstract def isOpen: Boolean
  abstract def write: Resource[IO, WriteTx]
  abstract def compute: Resource[IO, ReadTx]
}
