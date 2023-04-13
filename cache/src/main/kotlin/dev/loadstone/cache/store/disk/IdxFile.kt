package dev.loadstone.cache.store.disk

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.io.File
import java.io.FileNotFoundException
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

class IdxFile(val id: Int, private val file: File) : AutoCloseable {

    private val channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)

    val size get() = channel.size()

    fun read(containerId: Int): Index {
        val offset = containerId.toLong() * Index.SIZE.toLong()
        if(offset < 0 || offset >= channel.size()) {
            throw FileNotFoundException("Could not find container $containerId.")
        }
        val buf = Unpooled.buffer(Index.SIZE)
        buf.writeBytes(channel, offset, buf.writableBytes())
        return Index.decode(buf)
    }

    override fun close() {
        channel.close()
    }

    data class Index(val sector: Int, val size: Int) {

        fun encode(): ByteBuf {
            throw UnsupportedOperationException()
        }

        companion object {

            const val SIZE: Int = 6

            fun decode(buf: ByteBuf): Index {
                val size = buf.readUnsignedMedium()
                val sector = buf.readUnsignedMedium()
                return Index(sector, size)
            }
        }
    }
}