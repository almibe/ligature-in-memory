/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.slonky

import .ElementEncoding
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scodec.codecs.{byte, long}
import scodec.codecs.implicits.{implicitOptionCodec => _, _}

class EncodingSpec extends AnyFlatSpec with Matchers {
  "encodeSPOPrefix" should "create prefixes correctly" in {
    val e1 = Encoder.encodeSPOPrefix(1L, None, None, None)
    e1 shouldBe (byte ~~ long(64)).encode((3.toByte, 1)).require.bytes

    val e2 = Encoder.encodeOPSPrefix(1L, None, Some(2L), Some(ElementEncoding(1.toByte, 1L)))
    e2 shouldBe (byte ~~ long(64) ~~ byte ~~ long(64) ~~ long(64))
      .encode((8.toByte, 1L, 1.toByte, 1L, 2L)).require.bytes
  }
}
