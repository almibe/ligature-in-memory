/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.inmemory

import dev.ligature.LigatureSession
import dev.ligature.test.LigatureSuite

class InMemorySpec extends LigatureSuite {
  override def createSession(): LigatureSession = {
    val lim = new LigatureInMemory()
    lim.start()
  }
}
