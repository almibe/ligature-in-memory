/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.memory

import dev.ligature.LigatureStore
import dev.ligature.test.LigatureSuite

class InMemorySpec extends LigatureSuite {
  override def createStore(): LigatureStore = new InMemoryStore()
}
