/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import dev.ligature.store.keyvalue.Encoder.SPOC
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs.implicits.{implicitStringCodec => _, _}

import scala.util.{Failure, Success, Try}

object Decoder {
  private implicit val utf: Codec[String] = scodec.codecs.utf8

  def decodeSPOC(value: ByteVector): Try[SPOC] = {
    val res = Codec.decode[SPOC](value.bits)
    if (res.isSuccessful) {
      Success(res.require.value)
    } else {
      Failure(new RuntimeException("Invalid SPOC"))
    }
  }
}
