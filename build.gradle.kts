plugins {
    kotlin("jvm") version "2.3.21"
    kotlin("plugin.spring") version "2.3.21"
    kotlin("plugin.jpa") version "2.3.21"
    id("org.springframework.boot") version "4.1.0"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.osscontest"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    // Kafka listener 컨테이너와 관련 자동 설정을 함께 사용한다.
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    // MeterRegistry와 Kafka consumer 메트릭을 자동으로 등록한다.
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    // 이벤트 역직렬화에 사용하는 Jackson 3 ObjectMapper를 자동 설정한다.
    implementation("org.springframework.boot:spring-boot-starter-jackson")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.hibernate.orm:hibernate-vector")
    // Kotlin data class를 Jackson 3로 직렬화하고 역직렬화한다.
    implementation("tools.jackson.module:jackson-module-kotlin")
    runtimeOnly("org.postgresql:postgresql")

    // 원문 다운로드 (S3 호환 — MinIO 로컬 개발 포함)
    implementation(platform("software.amazon.awssdk:bom:2.29.1"))
    implementation("software.amazon.awssdk:s3")

    // 파서
    implementation("org.apache.pdfbox:pdfbox:3.0.3")
    implementation("org.apache.poi:poi-ooxml:5.3.0")
    // hwplib는 kr.dogfoot 그룹으로 Maven Central에 배포된다.
    implementation("kr.dogfoot:hwplib:1.1.9")

    // 토크나이저 (OpenAI cl100k_base 호환, FIXED_TOKEN 청킹에 사용)
    implementation("com.knuddels:jtokkit:1.1.0")

    // 임베딩
    implementation(platform("org.springframework.ai:spring-ai-bom:2.0.0"))
    implementation("org.springframework.ai:spring-ai-starter-model-openai")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    // Boot 4의 JPA 테스트 슬라이스 어노테이션을 제공한다.
    testImplementation("org.springframework.boot:spring-boot-starter-data-jpa-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("com.h2database:h2")
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.4.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
    }
}

tasks.test {
    useJUnitPlatform {
        excludeTags("integration")
    }
}

tasks.register<Test>("integrationTest") {
    group = "verification"
    description = "Runs tests that need external services such as Postgres or OpenAI."
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform {
        includeTags("integration")
    }
    shouldRunAfter(tasks.test)
}
