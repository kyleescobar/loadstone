package dev.loadstone.cache.settings

import dev.loadstone.cache.Archive
import dev.loadstone.cache.store.Container

data class ArchiveSettings(
    val version: Int?,
    val containsNameHash: Boolean,
    val containsSizes: Boolean,
    val containsUncompressedCrc: Boolean,
) {
    companion object {
        fun from(archive: Archive): ArchiveSettings {
            throw UnsupportedOperationException()
        }

        fun decode(container: Container): ArchiveSettings? {
            val buf = container.data
            val formatOpcode = buf.readUnsignedByte().toInt()
            val version = if(formatOpcode == 5) null else buf.readInt()
            val flags = buf.readUnsignedByte().toInt()
            val containsNameHash = flags and 0x01 != 0
            val containsSizes = flags and 0x04 != 0
            val containsUnknownHash = flags and 0x08 != 0
            val groupCount = buf.readUnsignedShort()
            val groupIds = IntArray(groupCount)
            var groupAccumulator = 0
            for(archiveIndex in groupIds.indices) {
                val delta = buf.readUnsignedShort()
                groupAccumulator += delta
                groupIds[archiveIndex] = groupAccumulator
            }
            val nameHashes = if(containsNameHash) IntArray(groupCount) {
                buf.readInt()
            } else null
            val compressedCrcs = IntArray(groupCount) { buf.readInt() }
            val uncompressedCrcs = if(containsUnknownHash) IntArray(groupCount) {
                buf.readInt()
            } else null
            val sizes = if(containsSizes) Array(groupCount) {
                Container.Size(compressed = buf.readInt(), uncompressed = buf.readInt())
            } else null
            val versions = Array(groupCount) { buf.readInt() }
            val fileIds = Array(groupCount) {
                IntArray(buf.readUnsignedShort())
            }
            for(group in fileIds) {
                var fileIdAccumulator = 0
                for(fileIndex in group.indices) {
                    val delta = buf.readUnsignedShort()
                    fileIdAccumulator += delta
                    group[fileIndex] = fileIdAccumulator
                }
            }
            val groupFileNameHashes = if(containsNameHash) {
                Array(groupCount) {
                    IntArray(fileIds[it].size) {
                        buf.readInt()
                    }
                }
            } else null
            val groupSettings = sortedMapOf<Int, Any>()
            return null
        }
    }
}