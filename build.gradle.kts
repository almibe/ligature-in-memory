plugins {
    id("org.jetbrains.kotlin.jvm").version("1.3.50")
}

repositories {
    mavenLocal()
    jcenter()
}

tasks.test {
    useJUnitPlatform()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    compile("org.almibe:ligature:0.1.0-SNAPSHOT")
    testImplementation("io.kotlintest:kotlintest-runner-junit5:3.3.0")
}
