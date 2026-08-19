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
    // Boot 4는 오토컨피그를 기능별 모듈로 쪼갰다 — 순수 org.springframework.kafka:spring-kafka만
    // 넣으면 KafkaAutoConfiguration(=ConcurrentKafkaListenerContainerFactory 빈, @EnableKafka)이
    // 전혀 로드되지 않아 @KafkaListener(Task 11의 IndexingKafkaListener)가 동작하지 않는다.
    // spring-boot-starter-kafka로 바꿔야 spring-kafka 자체와 오토컨피그를 함께 가져온다.
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    // 같은 이유로 ObjectMapper 빈(Task 11의 IndexingKafkaListener가 생성자로 주입받음)도
    // spring-boot-starter-jackson 없이는 오토컨피그되지 않는다(Jackson 3 기준 모듈).
    implementation("org.springframework.boot:spring-boot-starter-jackson")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.hibernate.orm:hibernate-vector")
    // 원래 스켈레톤(a5d44d4)의 build.gradle.kts에 tools.jackson.module:jackson-module-kotlin이
    // 있었다 — 이 프로젝트가 Jackson 3.x(패키지가 com.fasterxml.jackson.*가 아니라
    // tools.jackson.*)를 쓴다는 신호다. Kotlin data class를 Jackson으로 (역)직렬화하려면
    // (Task 11의 IndexingRequestedEvent 등) 이 모듈이 필요하다.
    implementation("tools.jackson.module:jackson-module-kotlin")
    runtimeOnly("org.postgresql:postgresql")

    // 원문 다운로드 (S3 호환 — MinIO 로컬 개발 포함)
    implementation(platform("software.amazon.awssdk:bom:2.29.1"))
    implementation("software.amazon.awssdk:s3")

    // 파서
    implementation("org.apache.pdfbox:pdfbox:3.0.3")
    implementation("org.apache.poi:poi-ooxml:5.3.0")
    // hwplib는 kr.dogfoot 그룹으로 Maven Central에 직접 배포된다 (jitpack 불필요, github.com/neolord0/hwplib는 별도 소스일 뿐 실제 배포 좌표는 kr.dogfoot).
    implementation("kr.dogfoot:hwplib:1.1.9")

    // 토크나이저 (OpenAI cl100k_base 호환, FIXED_TOKEN 청킹에 사용)
    implementation("com.knuddels:jtokkit:1.1.0")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    // Boot 4.1은 테스트 슬라이스 어노테이션(@DataJpaTest, @AutoConfigureTestDatabase)을
    // spring-boot-starter-test에서 분리해 이 스타터로 옮겼다
    // (패키지도 org.springframework.boot.data.jpa.test.autoconfigure / org.springframework.boot.jdbc.test.autoconfigure로 변경됨).
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
    description = "Runs tests that need a real Postgres (docker-compose)."
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform {
        includeTags("integration")
    }
    shouldRunAfter(tasks.test)
}
