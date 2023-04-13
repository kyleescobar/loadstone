plugins {
    kotlin("jvm")
}

allprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")

    group = "dev.loadstone"
    version = "211.1.0"

    repositories {
        mavenLocal()
        mavenCentral()
    }

    dependencies {
        implementation(kotlin("stdlib"))
        implementation(kotlin("reflect"))
    }

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(11))
        }
    }
}