package dev.loadstone.cache.store

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import io.netty.buffer.Unpooled
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import java.io.ByteArrayInputStream
import java.io.SequenceInputStream
import java.util.zip.GZIPInputStream

sealed class Compression(private val opcode: Int) {

    val headerSize get() = if(this !is Uncompressed) 4 else 0

    abstract fun decompress(data: ByteBuf, length: Int): ByteBuf

    companion object {
        fun fromOpcode(opcode: Int): Compression = when(opcode) {
            0 -> Uncompressed
            1 -> BZIP2
            2 -> GZIP
            else -> throw IllegalArgumentException("Unknown compression type with opcode: $opcode.")
        }
    }
}

object Uncompressed : Compression(opcode = 0) {
    override fun decompress(data: ByteBuf, length: Int): ByteBuf {
        return data.slice(data.readerIndex(), length)
    }
}

object BZIP2 : Compression(opcode = 1) {
    override fun decompress(data: ByteBuf, length: Int): ByteBuf {
        val out = Unpooled.buffer(length)
        SequenceInputStream(ByteArrayInputStream("BZh1".toByteArray(Charsets.US_ASCII)), ByteBufInputStream(data)).also {
            BZip2CompressorInputStream(it).use { input ->
                out.writeBytes(input, length)
            }
        }
        return out
    }
}

object GZIP : Compression(opcode = 2) {
    override fun decompress(data: ByteBuf, length: Int): ByteBuf {
        val out = Unpooled.buffer(length)
        GZIPInputStream(ByteBufInputStream(data)).use { input ->
            while(input.available() == 1) out.writeBytes(input, length)
        }
        return out
    }
}