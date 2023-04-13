package dev.loadstone.cache.store

import dev.loadstone.cache.store.disk.DatFile
import dev.loadstone.cache.store.disk.IdxFile
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.io.File
import java.io.FileNotFoundException

class FileStore private constructor(private val directory: File) : AutoCloseable{

    private val datFile = DatFile(directory.resolve(DAT_FILE_NAME))
    private val idxFiles = hashMapOf<Int, IdxFile>()

    init {
        if(!directory.exists()) {
            throw FileNotFoundException("Directory $directory does not exist.")
        }

        repeat(255) {
            val file = directory.resolve(IDX_FILE_NAME + "$it")
            if(!file.exists()) return@repeat
            idxFiles[it] = IdxFile(it, file)
        }
        idxFiles[255] = IdxFile(255, directory.resolve(IDX_FILE_NAME + "255"))
    }

    fun read(idxFileId: Int, containerId: Int): ByteBuf {
        val idxFile = idxFiles.getOrPut(idxFileId) { IdxFile(idxFileId, directory.resolve(IDX_FILE_NAME + "$idxFileId")) }
        val index = idxFile.read(containerId)
        if(index.size == 0) {
            return Unpooled.EMPTY_BUFFER
        }
        return datFile.read(idxFile.id, containerId, index)
    }

    override fun close() {
        datFile.close()
        idxFiles.values.forEach { it.close() }
    }

    companion object {

        const val DAT_FILE_NAME = "main_file_cache.dat2"
        const val IDX_FILE_NAME = "main_file_cache.idx"

        fun open(directory: File) = FileStore(directory)
    }
}