/**
 * The Lotse native protocol: frames, the field codec, error codes and the request and response types.
 *
 * <p>A frame is what one length-prefixed unit of the connection carries. Its length is a {@code u32} in
 * <b>big-endian</b> order (the one big-endian field of the protocol); everything after it is little-endian. See
 * {@link dev.lotse.proto.Frames} for the headers and {@link dev.lotse.proto.Reader} for the field conventions.
 *
 * <p>Types of the integers: {@code u8} is an {@code int} in {@code [0, 256)}, {@code u16} an {@code int} in
 * {@code [0, 65536)}, {@code u32} a {@code long} in {@code [0, 2^32)}, {@code u64} a {@code long} holding the
 * unsigned bit pattern (print it with {@link dev.lotse.record.Unsigned#toString(long)}). Optional fields are
 * {@code null} when absent.
 *
 * <p>Every message has its own layout, tagged-field blocks included: they are not at the same place in all
 * messages. The conformance vectors in {@code sdk/conformance} pin each layout to the broker's bytes.
 */
package dev.lotse.proto;
