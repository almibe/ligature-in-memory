/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import scodec.codecs.{byte, utf8, long, bytes}

object Encoders {
  val byteString = byte ~ utf8
  val byteBytes = byte ~ bytes
  val spoc = byte ~~ bytes ~~ byte ~~ long(64) ~~ long(64) ~~ byte ~~ long(64) ~~ long(64)
}
