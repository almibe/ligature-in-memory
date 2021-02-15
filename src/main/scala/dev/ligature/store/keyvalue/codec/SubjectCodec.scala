/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue.codec

import dev.ligature.{NamedNode, Node, AnonymousNode}
import scodec.bits.ByteVector

object SubjectCodec {
  def encode(subject: Node): ByteVector = {
    subject match {
      case n: NamedNode => NamedNodeCodec.encode(n)
      case a: AnonymousNode => AnonymousNodeCodec.encode(a)
    }
  }

  def decode(byteVector: ByteVector): Node = {
    ???
  }
}
