package dev.ligature.store.keyvalue.slonky

import scodec.bits.ByteVector

import scala.util.Try

/**
 * A trait that handles lower level access for working with LigatureStores.
 */
trait KeyValueStore {
  def get(key: ByteVector): Option[ByteVector]

  def put(key: ByteVector, value: ByteVector): Try[(ByteVector, ByteVector)]

  def delete(key: ByteVector): Try[ByteVector]

  def prefix(prefix: ByteVector): Iterable[(ByteVector, ByteVector)]

  def range(start: ByteVector, end: ByteVector): Iterable[(ByteVector, ByteVector)]
}
