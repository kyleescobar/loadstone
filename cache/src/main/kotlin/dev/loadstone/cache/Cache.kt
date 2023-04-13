package dev.loadstone.cache

import dev.loadstone.cache.settings.ArchiveSettings
import dev.loadstone.cache.store.Container
import dev.loadstone.cache.store.FileStore
import java.io.File

class Cache private constructor(
    val store: FileStore
) : AutoCloseable {

    val archiveCount get() = store.archiveCount



    override fun close() {
        store.close()
    }

    companion object {

        fun open(directory: File): Cache {
            val store = FileStore.open(directory)
            return Cache(store)
        }
    }
}

fun main() {
    println("Opening cache.")
    val cache = Cache.open(File("data/cache/"))
    val bytes = cache.store.read(255, 1)
    println("bytes: ${bytes.readableBytes()}")

    val container = Container.decode(bytes)
    val archiveSettings = ArchiveSettings.decode(container)
    println("Container")
}