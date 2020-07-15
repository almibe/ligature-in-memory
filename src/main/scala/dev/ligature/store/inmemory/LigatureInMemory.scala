/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.inmemory

import cats.effect.{IO, Resource}
import dev.ligature._

class LigatureInMemory extends Ligature {
  private val store = new InMemoryKeyValueStore()

  override def close() {
    store.close()
  }

  override def compute(): Resource[IO, ReadTx] = {
    store.compute
  }

  override def write(): Resource[IO, WriteTx] = {
    store.write
  }

  override def isOpen(): Boolean = store.isOpen
}
