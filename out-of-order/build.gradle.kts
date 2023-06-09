plugins {
    application
    id("java")
    id("com.github.davidmc24.gradle.plugin.avro") version "1.7.1"
}

group = "org.fred.demo"
version = "unspecified"

repositories {
    mavenCentral()
    maven( "https://packages.confluent.io/maven/")
}

dependencies {
    implementation("org.apache.kafka:kafka-streams:7.4.0-ccs")
    implementation("io.confluent:kafka-streams-avro-serde:7.4.0")

    implementation("io.confluent:kafka-avro-serializer:7.4.0")

    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.20.0")
    implementation("org.apache.logging.log4j:log4j-api:2.20.0")
    implementation("org.apache.logging.log4j:log4j-core:2.20.0")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.1")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:7.4.0-ccs")
    testImplementation("org.assertj:assertj-core:3.21.0")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

tasks.test {
    useJUnitPlatform()
}

application {
    mainClass.set("org.fred.demo.KsAppOOO")
}