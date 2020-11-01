/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package dev.ligature.store.keyvalue

import scodec.bits.{BitVector, ByteVector}
import scodec.{Attempt, Codec, DecodeResult, SizeBound}
import scodec.codecs.{long, utf8}

package object codec {
  private implicit val utf: Codec[String] = scodec.codecs.utf8

  private implicit val x: Codec[Option[Long]] = new Codec[Option[Long]] {
    override def decode(bits: BitVector): Attempt[DecodeResult[Option[Long]]] = ???

    override def encode(value: Option[Long]): Attempt[BitVector] = {
      value match {
        case Some(a) => long(64).encode(a)
        case None => Attempt.Successful(BitVector.empty)
      }
    }

    override def sizeBound: SizeBound = SizeBound.unknown
  }

  private implicit val y: Codec[Option[ElementEncoding]] = new Codec[Option[ElementEncoding]] {
    override def decode(bits: BitVector): Attempt[DecodeResult[Option[ElementEncoding]]] = ???

    override def encode(value: Option[ElementEncoding]): Attempt[BitVector] = {
      value match {
        case Some(a) => Codec.encode(a)
        case None => Attempt.Successful(BitVector.empty)
      }
    }

    override def sizeBound: SizeBound = SizeBound.unknown
  }
}
