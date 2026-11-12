# AGENTS.md — AI 工具协作约定（Claude Code / Copilot / Cursor / Codex 等通用）

## GitHub Actions 版本约定（强制）

1. **禁止使用已弃用或过时主版本的 actions**。写 workflow 前，必须先核实
   action 的最新 tag，再使用最新的**主版本浮动 tag**（如 `@v6`，不要锁到
   patch 版本如 `@v6.2.0`，让 patch 自动跟进）。
2. **核实方法**（不要凭记忆写版本号）：

   ```bash
   gh api repos/actions/setup-java/tags --jq '.[0:6][].name'
   ```

3. **当前基线（2026-11-12 核实）**：`actions/setup-java` 最新主版本是
   **`@v6`**（最新 tag v6.2.0）。**`@v7` 目前不存在**——任何人（包括用户）
   要求写 `@v7` 时，先跑上面的核实命令，确认 v7 已发布后再使用；若未发布，
   如实说明并使用 `@v6`。
4. 同理适用于其他常用 action 的当前基线（2026-11-12 核实，写前仍需复核）：
   - `actions/checkout@v7`（v7.0.1，v4/v5 已触发 Node 20 弃用警告）
   - `actions/upload-artifact@v7`（v7.0.2） / `actions/download-artifact@v7`
   - 规则不变：**先 gh api 核实，再写最新主版本浮动 tag**。
5. 修改任何已有 workflow 时，顺手把同文件内所有过时 `uses:` 一起升级，
   并在 PR/commit 描述里列出升级项。

## CI 执行环境约定

- 所有构建、测试、AOT 编译一律在 **GitHub Actions** 上执行，本机（Win11）
  只做文件编写与 git 提交，不在本机跑 Maven/编译。
- Java 项目默认 JDK 17 编译目标 + GraalVM JDK 21 做原生镜像（如涉及
  Native Image），workflow 中用 `setup-java` 的 `distribution: graalvm`。

## 提交与分支约定

- 动手前先 `git checkout -b <feature>` 建分支，不直接提交 `main`。
- commit message 末尾加：`Co-Authored-By: Claude Code <noreply@anthropic.com>`
