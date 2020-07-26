/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.inmemory

import dev.ligature.store.inmemory.InMemoryKeyValueStore.ByteVectorOrdering
import dev.ligature.store.keyvalue.Encoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scodec.bits.ByteVector
import scodec.codecs.byte

class EncodingSpec extends AnyFlatSpec with Matchers {
  "encodeSPOPrefix" should "create prefixes correctly" in {
    val e = Encoder.encodeSPOPrefix(1L, None, None, None)
    println("** " + e)
  }
}
