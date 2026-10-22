plugins {
    java
    application
}

group = "dev.raftkv"
version = "1.0.0"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.release.set(21)
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed", "skipped")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
    systemProperty("junit.jupiter.execution.timeout.default", "90s")
}

application {
    mainClass.set("raftkv.server.Main")
    applicationDefaultJvmArgs = listOf("-Dsun.net.httpserver.nodelay=true")
}

tasks.register<JavaExec>("bench") {
    group = "application"
    description = "Measure throughput and latency and write results/benchmarks.json"
    mainClass.set("raftkv.bench.Benchmark")
    classpath = sourceSets.main.get().runtimeClasspath
    workingDir = rootProject.layout.projectDirectory.asFile
    jvmArgs("-Xms256m", "-Xmx2g", "-Dsun.net.httpserver.nodelay=true")
}

tasks.register<JavaExec>("traces") {
    group = "application"
    description = "Record visualizer traces under web/public/traces"
    mainClass.set("raftkv.sim.TraceGen")
    classpath = sourceSets.main.get().runtimeClasspath
    workingDir = rootProject.layout.projectDirectory.asFile
}

tasks.register<JavaExec>("chaos") {
    group = "application"
    description = "Run the chaos campaign and write results/simulation.json"
    mainClass.set("raftkv.sim.ChaosCampaign")
    classpath = sourceSets.main.get().runtimeClasspath
    workingDir = rootProject.layout.projectDirectory.asFile
    jvmArgs("-Xms256m", "-Xmx2g")
}
