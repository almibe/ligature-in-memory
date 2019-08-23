plugins {
    id("org.jetbrains.kotlin.jvm").version("1.3.50")
}

repositories {
    mavenLocal()
    jcenter()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    compile("com.google.guava:guava:28.0-jre")
    compile("org.almibe:ligature:0.1.0-SNAPSHOT")
    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
}
