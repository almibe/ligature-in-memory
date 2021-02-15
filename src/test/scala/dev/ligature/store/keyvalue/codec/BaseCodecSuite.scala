/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.codec

import dev.ligature.NamedNode
import munit.FunSuite
import dev.ligature.store.keyvalue.codec.NamedNodeCodec._

class BaseCodecSuite extends FunSuite {
  val namedNodes = List(
    NamedNode("hello")
  )

  test("test codec") {

    namedNodes.foreach { namedNode =>
      assertEquals(namedNode, decode(encode(namedNode)))
    }
  }
}
