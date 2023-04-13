package dev.loadstone.cache.store

import dev.loadstone.cache.util.XTEA_ZERO_KEY
import dev.loadstone.cache.util.isZeroKey
import dev.loadstone.cache.util.xteaDecrypt
import io.netty.buffer.ByteBuf
import io.netty.buffer.DefaultByteBufHolder
import io.netty.buffer.Unpooled

data class Container(
    val data: ByteBuf,
    val compression: Compression = Uncompressed,
    val version: Int? = null
) : DefaultByteBufHolder(data) {

    val isVersioned get() = version != null

    data class Size(
        val compressed: Int,
        val uncompressed: Int
    )

    companion object {

        const val XTEA_HEADER_SIZE = 5

        fun decode(buf: ByteBuf, xteaKey: IntArray = XTEA_ZERO_KEY): Container {
            val compression = Compression.fromOpcode(buf.readUnsignedByte().toInt())
            val compressedSize = buf.readInt()
            val encCompSize = compression.headerSize + compressedSize
            val decBuf = if(!xteaKey.isZeroKey) {
                buf.xteaDecrypt(xteaKey, end = buf.readerIndex() + encCompSize)
            } else buf.slice(buf.readerIndex(), buf.readableBytes())
            val decompBuf = if(compression != Uncompressed) {
                val uncompressedSize = decBuf.readInt()
                val uncompressed = try {
                    compression.decompress(decBuf, uncompressedSize)
                } catch(e: Exception) {
                    throw IllegalStateException("Failed to decrypt container data using XTEA: (${xteaKey.contentToString()}).")
                }
                Unpooled.wrappedBuffer(uncompressed)
            } else decBuf.slice(0, compressedSize)
            decBuf.readerIndex(encCompSize)
            val version = if(decBuf.readableBytes() >= 2) decBuf.readShort().toInt() else null
            return Container(decompBuf, compression, version)
        }
    }
}