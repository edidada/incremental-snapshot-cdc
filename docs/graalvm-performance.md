# 用 GraalVM 优化 Java Web 与大数据项目的性能

> 实操对象：本仓库（`incremental-snapshot-cdc`）。
> 配套阅读：`test_java_graalvm/docs/graalvm-native-image.md`（GraalVM 目标与现状）。

## 0. 先分清两类"性能"

GraalVM 有两个独立武器，别混用：

| 武器 | 解决什么 | 不解决什么 |
|------|---------|-----------|
| **Native Image（AOT）** | 启动延迟、冷启动、内存占用（RSS）、镜像体积 | 长跑峰值吞吐（通常还略降，除非 PGO） |
| **Graal JIT**（GraalVM 作为运行时） | 长跑峰值吞吐（编译质量高于 C2） | 启动速度 |

选型口诀：**"启动慢/内存贵 → AOT；跑得久/吞吐高 → Graal JIT"**。两者可叠加
（用 GraalVM JDK 构建原生镜像，也用 GraalVM JDK 跑长跑服务）。

## 1. Java Web 项目（Spring Boot 3 / Quarkus / Micronaut）

1. **冷启动敏感场景必上 Native Image**：Serverless、K8s 高频扩缩容、
   API 网关旁路服务、CLI 工具。
   - Spring Boot 3：`mvn -Pnative package`（需 GraalVM JDK），反射/JNI/资源
     由 Boot 的 AOT 引擎在构建期生成 hint，业务代码基本零改动。
   - Quarkus：设计即 native-first，兼容性最好，`quarkus:native-image` 一键。
2. **JVM 模式下也有立竿见影的收益**（不换二进制形态）：
   - `-XX:+UseSerialGC` 或小容器配 `-Xmx` 收紧 + `-XX:TieredStopAtLevel=1`
     （短命进程）；长跑服务相反，让 C2 充分预热。
   - 用 `-XX:+UseStringDeduplication`（G1）降内存；堆外统计用 Native Memory Tracking。
   - 分层启动：CRaC（checkpoint/restore）与 AppCDS 是 Native Image 之外的
     官方"快速启动"补充。
3. **验证指标**：CI 里用 `/usr/bin/time` 测启动、`RSS`（`/proc/<pid>/status`）
   测内存——见本仓库 `.github/workflows/graalvm.yml` 的 benchmark 步骤。

## 2. 大数据项目（Flink/Spark/Kafka Connect）

大数据框架几乎全是反射/动态代理/序列化/`ServiceLoader` 驱动，**整框架上
Native Image 目前不现实**（Flink/Spark 官方均未支持）。正确姿势是"逐层拆"：

1. **自定义算子/连接器抽成无框架依赖的核心库**（本仓库就是这么设计的：
   `cdc-core` 零依赖、零反射）→ 这层可直接 AOT 编译成原生 CLI/daemon。
2. **框架宿主保持 JVM + Graal JIT**：TaskManager/Executor 这种长跑进程，
   峰值吞吐优先，JVM 模式是正确形态；换 GraalVM 企业版 JIT（或未来 OpenJDK
   里的 jdk.graal 编译器）可拿到 5%~15% 稳态吞吐。
3. **边缘/嵌入式采集端用 Native Image**：CDC 采集 agent、日志 tailer、
   单机同步工具这类"小而常驻"的进程，AOT 收益最大（RSS 从数百 MB 降到
   数十 MB，可塞进资源受限节点）。
4. **JNI 密集型组件**（RocksDB state backend、arrow-jni）：Native Image 下
   需要 `--trace-object` 类的元数据支持，短期内建议留在 JVM 模式。

## 3. 本仓库的 GraalVM 改造清单

| 文件 | 改动 |
|------|------|
| `cdc-core/pom.xml` | 新增 `native` profile：`org.graalvm.buildtools:native-maven-plugin`（0.10.6），主类 `IncrementalSnapshotDemo`，`-O3 --gc=serial -march=compatibility --no-fallback` |
| `.github/workflows/graalvm.yml` | 新 workflow：GraalVM JDK 21 → JVM 模式全量测试 → `mvn -Pnative -pl cdc-core -am package` 构建 ELF 原生二进制 → 自校验 demo 运行 → JVM vs Native 启动耗时各跑 5 轮 → 上传产物、写 Step Summary |
| `docs/graalvm-performance.md` | 本文档 |

**为什么这个仓库零改动即可 AOT**：closed-world 可达性分析要求所有反射/动态
行为在构建期可见。`cdc-core` 无 `Class.forName`、无 `ServiceLoader`、无
JNI、无运行时资源文件加载（已 grep 验证），因此**无需任何
`reflect-config.json` / `resource-config.json`**。

**构建命令（CI 内执行，本机不需要装 GraalVM）**：

```bash
mvn -B -ntp -Pnative -pl cdc-core -am package -DskipTests
./cdc-core/target/cdc-demo
```

## 4. 预期结果与验收口径

- 产物：Linux ELF 原生可执行文件（`cdc-core/target/cdc-demo`），无需 JRE。
- 启动：demo 端到端运行，JVM 模式约 0.3~1.5s（含 JIT 预热），原生模式
  预期 <100ms（数量级改善）。
- 内存：原生镜像进程 RSS 预期 <50MB（Serial GC + 无 JIT 元数据）。
- 体积：原生二进制约 10~20MB；对比 JVM 模式需要 JRE（约 50MB+ 运行时）。
- CI 红线：native demo 自校验失败或 JVM 测试不过，workflow 即失败。

以上数字以 workflow 实际输出为准（见 Step Summary / run log）。
