# service-mesh-request-retry-context-validator

验证重试上下文、请求体摘要和跨服务传播。场景是一个重试请求经过
`网关 -> 服务 A -> 服务 B`：第一跳可能已经解压请求体，而幂等键只在入口生成
一次。下游各自重试时，容易因为摘要对不上、键没有传播，或在副作用已提交后
盲目重放，造成重复扣款、重复下单等问题。

本仓库用纯 Python 标准库对这些规则做**离线验证**，输入原请求、请求体摘要、
幂等键、调用链、重试次数、attempt ID 和响应状态，逐跳输出是否允许重试、
可重试位置，以及重复副作用风险。

> 本工具**不连接真实代理**，是纯本地规则校验器 / 测试替身：不连接、不代理
> 任何真实 Service Mesh 或代理（Envoy、Sidecar、网关等），不发起任何网络
> 请求。所有“传播”“存储”都是进程内模拟，见
> `retryctx/cancellation.py` 与 `retryctx/idempotency.py`。

## 运行环境与测试

- Python 3.8.2（仅使用标准库；在 3.8.2 与更新版本上均验证通过）

```bash
python -m unittest
```

默认 discovery 会运行 `tests/` 下全部用例，覆盖：

- `tests/test_digest_change.py`：gzip 解压后 **wire 摘要变化**、规范化摘要
  保持稳定、请求体被篡改后摘要不一致、幂等存储对“同键不同体”返回冲突；
- `tests/test_retry_chain.py`：网关和两个服务的**重试链**，共享幂等键、预算
  耗尽、键缺失/不一致、副作用导致的逐跳降级与上游连带风险；
- `tests/test_cancellation.py`：**取消晚到**、取消传播所有分支、显式到达列表
  缺分支、不可达分支；
- `tests/test_idempotent_execution.py`：模拟副作用属主，证明同键同摘要重试只
  执行一次、同键异体返回冲突且不会复用结果；
- `tests/test_cli_examples.py`：三个示例 JSON 与 CLI 冒烟。

## 它验证的规则

1. **同一逻辑请求共享一个幂等键**：键要么由调用方传入，要么只在入口
   （网关）生成一次（`idempotency_key_source = generated_at_entry`）。链路上
   每个 attempt 都必须携带同一个键；缺失报 `IDEMPOTENCY_KEY_MISSING`，不一致
   报 `IDEMPOTENCY_KEY_MISMATCH`，下游自行生成新键报
   `KEY_GENERATED_DOWNSTREAM`，该跳直接阻止重试。
2. **不同请求体不能复用结果**：幂等记录同时绑定键和**规范化请求体摘要**
   （解压后的逻辑体 SHA-256）。同键同摘要才可重放；同键不同摘要返回
   `conflict`，禁止复用首次结果。attempt 摘要对不上时该跳
   `BODY_DIGEST_MISMATCH` 并被阻止。
3. **解压导致的摘要变化被正确区分**：压缩字节的 `wire_digest` 与解压后逻辑体
   的 `canonical_digest` 本来就不同，报告在
   `digest.digest_changed_after_decompression` 中说明；若入口错误地把记录绑定
   到压缩 wire 摘要，会报 `STALE_WIRE_DIGEST_BINDING`。
4. **已产生副作用的 attempt 不能直接重放**：
   - 副作用未受幂等去重保护，且响应超时/5xx/结果未知，或其上游结果不确定：
     该跳及所有会重新执行到它的上游位置降级为 `blocked`，风险等级 `high`，
     要求先查询/对账并补上去重；
   - 副作用声明受同一个幂等键保护：位置降级为 `idempotent_retry_only`
     （`direct_replay_allowed=false`），只能带键让副作用属主去重，风险降为
     `low`。
5. **重试次数预算**：`retry_count` 是每跳最大重试次数（也可用
   `max_retries` 单独覆盖某跳）；已用次数取 attempt 序号与记录条数-1 的较大
   值，超额报 `RETRY_BUDGET_EXHAUSTED`。
6. **取消传播所有分支**：取消后任何跳都不能再发起新 attempt
   （统一加 `CANCELLATION_REQUESTED` 并置为 `blocked`）；所有运行中的分支被
   标记为 `cancelled`。取消晚到时，已提交副作用的分支进入 `late_arrivals`
   并给出补偿/对账风险，其余分支仍必须全部收到取消。显式 `reached` 列表漏掉
   分支或分支不可达时，`propagated_to_all=false` 并报错。

响应状态层面，`None`（超时/连接失败，没有响应）与 `408/425/429/500/502/503/504`
属于“状态码本身允许考虑重试”，`2xx` 为 `not_required`，其余状态为
`STATUS_NOT_RETRYABLE`；最终是否可重试还必须通过上面的键、摘要、副作用和取消
规则。

## 每跳决策取值

| decision | 含义 | 是否出现在 `retryable_positions` |
| --- | --- | --- |
| `retryable` | 可直接重试（键/摘要一致、有预算、状态可重试、无副作用约束、未取消） | 是，`mode=direct` |
| `idempotent_retry_only` | 已有提交的副作用，禁止直接重放，只能凭同一幂等键去重重试 | 是，`mode=idempotent_dedup` |
| `blocked` | 键/摘要/预算/状态/未保护副作用/取消等原因阻止自动重试 | 否 |
| `not_required` | 最后一次 attempt 已成功 | 否 |
| `no_attempt_recorded` | 该位置还没有 attempt，无“重试”可言 | 否 |

## 编程接口

七个输入可以直接作为关键字参数传给 `evaluate_retry`：

```python
from retryctx import evaluate_retry

report = evaluate_retry(
    original_request={                 # 原请求（也可传 HttpRequest）
        "method": "POST",
        "path": "/pay",
        "body_text": '{"amount":99}',
        "compress": "gzip",            # 第一跳会解压
    },
    request_body_digest="sha256:...",  # 请求体摘要（规范化/解压后）
    idempotency_key="idem-...",        # 不传则只在入口生成
    call_chain=[...],                  # 调用链，每跳含 attempts
    retry_count=2,                     # 最大重试次数
    attempt_id="pay-1",                # 要定位的当前 attempt ID
    response_status=None,              # 该 attempt 的响应状态（None=超时）
)

report["idempotency_key"]               # 全链共享的幂等键
report["retryable_positions"]           # 可重试位置及 direct/idempotent_dedup 模式
report["duplicate_side_effect_risks"]   # 重复副作用风险（high/medium/low）
```

也可以使用完整场景对象：

```python
from retryctx import RetryContextValidator

report = RetryContextValidator().validate(scenario_dict)
```

幂等存储模拟了“键 + 摘要”绑定的去重记录：

```python
from retryctx import IdempotencyStore, REPLAY, CONFLICT

store = IdempotencyStore()
store.begin("idem-1", digest_a)
store.complete("idem-1", digest_a, 200, {"order_id": "O-1"})
store.lookup("idem-1", digest_a).outcome  # REPLAY
store.lookup("idem-1", digest_b).outcome  # CONFLICT：不同请求体不得复用结果
```

## 命令行

```bash
# JSON 报告（默认）
python -m retryctx examples/retry-chain.json

# 人类可读文本
python -m retryctx examples/cancellation-late.json --format text

# 内置演示 / 从 stdin 读取
python -m retryctx --demo --format text
cat examples/digest-decompression.json | python -m retryctx
```

仓库自带三个场景：

- `examples/digest-decompression.json`：解压后 wire 摘要变化、canonical 摘要
  一致，两跳均可直接重试；
- `examples/retry-chain.json`：支付超时但扣款已发生且无去重，三跳全部阻止，
  输出 `high` 重复副作用风险；
- `examples/cancellation-late.json`：取消晚到，支付分支已扣款，另外两个分支
  仍被取消，所有重试位置关闭。

## 输出结构（要点）

```json
{
  "idempotency_key": "idem-...",
  "idempotency_key_source": "provided | generated_at_entry",
  "canonical_body_digest": "sha256:...",
  "wire_body_digest": "sha256:...",
  "digest": { "digest_changed_after_decompression": true },
  "positions": [ { "position": "...", "decision": "...", "blockers": [] } ],
  "retryable_positions": [
    { "position": "...", "mode": "direct | idempotent_dedup" }
  ],
  "duplicate_side_effect_risks": [
    {
      "risk_level": "high | medium | low",
      "attempt_id": "...",
      "side_effect": "...",
      "idempotency_key": "idem-...",
      "would_retrigger_positions": ["..."],
      "required_action": "..."
    }
  ],
  "cancellation": { "propagated_to_all": true, "late_arrivals": [] },
  "findings": [ { "code": "...", "severity": "error|warning|info" } ]
}
```

## 目录结构

```text
retryctx/
  digest.py          # wire/canonical 摘要、gzip/deflate 解码
  idempotency.py     # 入口键生成 + 键/摘要绑定的内存去重存储
  cancellation.py    # 全分支取消传播与晚到处理
  model.py           # 请求、调用链、attempt、副作用、分支模型与 JSON 解析
  validator.py       # 逐跳重试决策与风险汇总（evaluate_retry 七输入 API）
  cli.py             # 本地 JSON/text 命令行
tests/               # python -m unittest 用例
examples/            # 三个可直接运行的场景
```
