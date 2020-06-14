plugins {
    id("org.jetbrains.kotlin.jvm").version("1.3.70")
    maven
}

project.group = "dev.ligature"
project.version = "0.1.0-SNAPSHOT"

repositories {
    mavenLocal()
    jcenter()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.4")
    implementation("io.vavr:vavr:0.10.3")
    implementation("dev.ligature:ligature:0.1.0-SNAPSHOT")
    testImplementation("dev.ligature:ligature-test-suite:0.1.0-SNAPSHOT")
    testImplementation("io.kotlintest:kotlintest-runner-junit5:3.4.2")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
