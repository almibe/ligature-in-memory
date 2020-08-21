/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import scodec.Codec
import scodec.codecs.utf8

package object codec {
  private implicit val utf: Codec[String] = utf8

}
