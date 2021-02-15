/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.codec

import dev.ligature.AnonymousNode
import munit.FunSuite
import dev.ligature.store.keyvalue.codec.AnonymousNodeCodec._

class AnonymousNodeCodecTest extends FunSuite {
  test("test codec") {
    val anonymousNodes = List(
      AnonymousNode(12L)
    )

    anonymousNodes.foreach { namedNode =>
      assertEquals(namedNode, decode(encode(namedNode)))
    }
  }
}
