package dev.loadstone.cache.store.disk

import io.netty.buffer.ByteBuf
import io.netty.buffer.DefaultByteBufHolder
import io.netty.buffer.Unpooled
import java.io.File
import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

class DatFile(private val file: File) : AutoCloseable {

    private val channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)

    val size get() = channel.size()

    fun read(idxFileId: Int, containerId: Int, index: IdxFile.Index): ByteBuf {
        val buf = Unpooled.buffer(index.size / Sector.SIZE)
        var sectorsRead = 0
        var bytesRemaining = index.size
        var curOffset = index.sector * Sector.SIZE.toLong()
        do {
            val sector = readSector(containerId, curOffset)
            sector.validate(idxFileId, containerId, sectorsRead)
            if(bytesRemaining > sector.data.writerIndex()) {
                buf.writeBytes(sector.data)
                bytesRemaining -= sector.data.writerIndex()
                sectorsRead++
                curOffset = sector.id * Sector.SIZE.toLong()
            } else {
                buf.writeBytes(sector.data.slice(0, bytesRemaining))
                bytesRemaining = 0
            }
        } while(bytesRemaining > 0)
        return buf
    }

    private fun readSector(containerId: Int, offset: Long): Sector {
        val buf = Unpooled.buffer(Sector.SIZE)
        buf.writeBytes(channel, offset, buf.writableBytes())
        return Sector.decode(buf)
    }

    override fun close() {
        channel.close()
    }

    data class Sector(
        val idxFileId: Int,
        val containerId: Int,
        val id: Int,
        val offset: Int,
        val data: ByteBuf
    ) : DefaultByteBufHolder(data) {

        fun encode(): ByteBuf {
            throw UnsupportedOperationException()
        }

        fun validate(idxFileId: Int, containerId: Int, offset: Int) {
            if(this.idxFileId != idxFileId) throw IOException("IDX id mismatch. (${this.idxFileId}, $idxFileId)")
            if(this.containerId != containerId) throw IOException("Container id mismatch. (${this.containerId}, $containerId)")
            if(this.offset != offset) throw IOException("Sector offset mismatch. (${this.offset}, $offset)")
        }

        companion object {

            const val HEADER_SIZE = 8
            const val DATA_SIZE = 512
            const val SIZE = HEADER_SIZE + DATA_SIZE

            fun decode(buf: ByteBuf): Sector {
                val containerId = buf.readUnsignedShort()
                val offset = buf.readUnsignedShort()
                val id = buf.readUnsignedMedium()
                val idxFileId = buf.readUnsignedByte().toInt()
                val data = buf.slice(HEADER_SIZE, DATA_SIZE)
                return Sector(idxFileId, containerId, id, offset, data)
            }
        }
    }
}