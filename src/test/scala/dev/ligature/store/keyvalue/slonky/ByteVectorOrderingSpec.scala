/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.slonky

import dev.ligature.store.keyvalue.slonky.InMemoryKeyValueStore.ByteVectorOrdering
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scodec.bits.ByteVector
import scodec.codecs.byte

import scala.collection.mutable

class ByteVectorOrderingSpec extends AnyFlatSpec with Matchers {
  "ByteVectorOrdering" should "order ByteVectors lexicographically" in {
    val encoder2 = byte ~~ byte
    val encoder = byte ~~ byte ~~ byte

    val bv1 = encoder.encode(0.toByte, 0.toByte, 0.toByte).require.bytes
    val bv2 = encoder.encode(0.toByte, 0.toByte, 1.toByte).require.bytes
    ByteVectorOrdering.compare(bv1, bv2) shouldBe -1

    val bv3 = encoder.encode(0.toByte, 1.toByte, 1.toByte).require.bytes
    val bv4 = encoder2.encode(0.toByte, 1.toByte).require.bytes
    ByteVectorOrdering.compare(bv3, bv4) shouldBe 1

    val t = mutable.TreeMap[ByteVector, ByteVector]()(ByteVectorOrdering)
    t.put(bv1, bv1)
    t.put(bv2, bv2)
    t.range(ByteVector.fromByte(0.toByte), ByteVector.fromByte(3.toByte)).size shouldBe 2
  }
}
