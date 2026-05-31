# 无排序 K-连续段检测（Halo 编码桶级聚合）

> 一种通用的分布式 SQL 算法，用于在海量整数点轨迹中查找长度 ≥ K 的连续整数段
> —— 全程无需排序。适用于任何支持 `UNION ALL`、`GROUP BY` 和条件聚合的引擎
> （SCOPE / Cosmos、MaxCompute / ODPS、Spark SQL、Hive、BigQuery、Trino、
> ClickHouse 等）。

[![Algorithm](https://img.shields.io/badge/算法-Halo%20编码桶级聚合-blue)]()
[![Status](https://img.shields.io/badge/状态-实验性-orange)]()
[![License](https://img.shields.io/badge/协议-MIT-green)]()

---

## 一句话总结

给定 `N` 条整数点轨迹，每条包含 `M` 个（可能重复的）整数点，判断每条轨迹中
是否存在长度 ≥ K 的连续整数段，若存在则输出该段的一个起点。

| 输入 | 输出 |
| --- | --- |
| `(group_id, x)` | `(group_id, start_x)` |

经典 SQL 做法是 **gaps-and-islands** —— 基于 `ROW_NUMBER()` / `LAG()` 的排序
方案，每组代价 `O(M log M)`。本仓库实现了一种 **无排序** 变体：通过 `UNION ALL`
扇出指示器行 + 两层 `GROUP BY` 聚合完成检测。每组时间 `O(M)`，每行状态 `O(1)`，
整个流水线天然可并行（每个 `(group_id, bucket)` 对独立计算）。

---

## 文件结构

| 文件 | 引擎 | 说明 |
| --- | --- | --- |
| [dist_k_seg_finder_agg.script](dist_k_seg_finder_agg.script) | SCOPE | 端到端脚本：数据生成 + 聚合算法 + 排序验证 + diff |
| [dist_k_seg_finder_agg.sql](dist_k_seg_finder_agg.sql) | MaxCompute SQL | 相同算法的标准 SQL 版本；可移植到 Spark / Hive / Trino |
| `README.md` | — | 英文版文档 |
| `README.zh.md` | — | 本文件（中文版） |

两种实现在数学上完全等价，且都附带排序验证器用于交叉校验。

---

## 算法详解

### Step 0 — Halo 扇出（5 路指示器）

每个原始点 `x` 被展开为 5 条指示器行，偏移量为 `{0, +1, -1, -K, -(K+1)}`：

| `temp_x` | 指示器 | 含义 |
| --- | --- | --- |
| `x`       | `I_raw = 1` | 位置 `t` 有原始点 |
| `x + 1`   | `I_d1 = 1`  | 位置 `t-1` 有原始点 |
| `x - 1`   | `I_p1 = 1`  | 位置 `t+1` 有原始点 |
| `x - K`   | `I_k = 1`   | 位置 `t+K` 有原始点 |
| `x - K-1` | `I_kp1 = 1` | 位置 `t+K+1` 有原始点 |

然后 `GROUP BY (group_id, temp_x)` 并用 `MAX`（逻辑 OR）合并指示器。
聚合后，整数轴上每个位置 `t` 在 `O(1)` 列中即可获知其邻域
`{t, t±1, t+K, t+K+1}` 的原始点分布。

### Step 1 — 桶级聚合（`bucket = ⌊temp_x / K⌋`）

将行按宽度 `K` 分桶。对每个桶 `B`，定义锚点 `M = B*K + K - 1`（桶左半窗
的最后一格）。每桶计算三个值：

```
has_M_raw = MAX( I_raw=1 AND temp_x = M )
L_M       = MAX( temp_x  : I_raw=1 AND I_d1=0  AND temp_x ≤ M )
R_M       = MIN( 2K 窗口内的端点候选 )
```

**关键引理：** 长度 ≥ K 的连续段 `[s, e]` 的起点 `s` 落在桶 `B` 左半窗
⇔ 锚点 `M` 落在该段中 ⇔ `I_raw(M) = 1` 且包含 `M` 的极大 raw run
长度 ≥ K。

**为何不需要逐点检查：** 定义 `L_M` 的 `MAX` 自动落在包含 `M` 的 run 的
起点上——因为 run 内部的其它位置都满足 `I_d1 = 1`（左邻居也是 raw），会被
排除。`R_M` 同理。

**唯一报告权：** 若 run 起点 `s < B*K`，则桶 `B` 的 `[B*K, M]` 范围内全部
为 raw，没有任何 `t` 满足 `I_d1(t) = 0`，因此 `L_M = NULL`，桶 `B` 不报告。
拥有 `s` 的桶 `⌊s/K⌋` 会负责报告。这保证了每个 run 起点被恰好一个桶报告。

### Step 2 — 组级去重

同一 group 可能有多个独立长 run 分别被各自桶报告。题目只需每组一个起点：

```sql
SELECT group_id, MIN(start_x) FROM hits GROUP BY group_id
```

---

## 复杂度对比

| | 无排序聚合（本算法） | 经典排序 + 扫描 |
| --- | --- | --- |
| 每组时间 | `O(M)` | `O(M log M)` |
| 每行状态 | `O(1)` | `O(1)`（排序后） |
| 集群 shuffle | 按 `(group_id, temp_x)` 再按 `(group_id, bucket)` 哈希 | 按 `(group_id, x)` 排序 |
| 倾斜敏感度 | 低 —— 工作量与每桶 **distinct** `temp_x` 成比例 | 高 —— 一个大 group 阻塞整个 reducer |
| 行膨胀倍数 | 5× | 1× |

5 倍膨胀是跳过排序的代价。在存在严重 per-group 倾斜的场景下，这通常是划算的。

---

## 应用场景

| 场景 | 说明 |
| --- | --- |
| **用户连续活跃天数检测** | 判断用户是否有连续 K 天登录/消费，无需对日期排序 |
| **连续 Session 识别** | 在点击流中找出连续 K 个时间槽有行为的 session |
| **基因组连续覆盖检测** | 测序数据中找连续 K 个碱基位点被覆盖的区域 |
| **库存/供应链连续缺货** | 检测连续 K 天缺货的 SKU |
| **网络监控连续丢包** | 在海量探测点中找连续 K 个时隙异常的链路 |
| **推荐系统连续曝光未点击** | 检测 item 连续 K 次曝光无交互（疲劳信号） |

---

## 最佳优化场景

**大规模分布式系统（SCOPE / Spark / Hive）中、per-group 基数很大、且数据已按
GroupId 哈希分区但未按 x 排序的场景**，优化幅度最大：

1. **避免 PRESORT 的 shuffle-sort 开销**
   传统做法是 `REDUCE ON GroupId PRESORT x ASC`，需要全量排序 `O(M log M)`
   per group，在分布式环境中还意味着额外的 shuffle + 排序网络开销。本算法只需
   GROUP BY（哈希聚合），在已按 GroupId 分区的数据上几乎免费。

2. **数据倾斜（data skew）场景**
   排序型 Reducer 如果某个 GroupId 有百万级点，单个 reducer 成为瓶颈。本算法的
   GROUP BY 可以利用 combiner / partial aggregation，天然抗倾斜。

3. **M 极大、K 相对小**
   例如每个用户有 $10^5 \sim 10^6$ 个事件点，K 只有 7~30（连续天数）。5 倍
   数据膨胀的代价远小于排序 M log M 的代价，且膨胀后的 GROUP BY 可在 map 端
   做 partial combine。

4. **流水线中数据已经是 flat 表**
   上游输出已是 `(GroupId, x)` 的无序 flat 格式时，传统方法必须加排序步骤；
   本算法直接接入，省掉一个完整的 stage。

### 量化直觉

$$
\text{传统} = O(M \log M) \text{ sort} + O(M) \text{ scan} \quad \xrightarrow{\text{本算法}} \quad O(5M) \text{ hash agg (两层)}
$$

当 $M = 10^6, K = 10$ 时，$\log M \approx 20$，理论加速约 **4×**；但在分布式
场景中，省掉 sort stage 带来的 **网络 I/O 和调度延迟节省** 往往远超算术加速，
实际可达 **数倍到一个数量级** 的端到端提升。

---

## 交叉验证

两种实现都附带排序验证器（gaps-and-islands 方案），对每个 group 输出第一个满足
条件的 run 起点。通过 `FULL OUTER JOIN` on `group_id` 产生 diff 流，标记为：

- `OK` — 两种算法一致（从 diff 输出中过滤掉）
- `ONLY_AGG` / `ONLY_SORT` — 一种命中、另一种未命中
- `DIFF_RUN` — 两者都命中但起点相差 ≥ K（属于不同 run）。差值 < K 的视为合法
  （同一 run 内任意起点均可接受）

`K` 参数在每个脚本中有唯一数据源（SCOPE 中为 `#DECLARE`，MaxCompute SQL 中为
内联字面量），两种算法不会因 `K` 漂移而不一致。

---

## 输出

所有输出为 TSV 格式，方便导入 pandas / Excel：

```
seg_result        -- 聚合算法结果     (group_id, start_x)
seg_result_sort   -- 排序验证器结果   (group_id, start_x)
seg_result_diff   -- 仅非 OK 行      (group_id, start_agg, start_sort, status)
```

---

## 参数

```
N      = 轨迹（组）数量
M      = 每条轨迹的点数
K      = 最小连续段长度        <-- 算法唯一依赖的参数
XRange = x ∈ [0, XRange)      <-- 仅用于合成数据生成
Seed   = 随机种子（可复现）
```

SCOPE 脚本中 `K` 是 `#DECLARE`；MaxCompute SQL 中是内联字面量 `10` ——
修改一处即可。

---

## 使用方法

### SCOPE

1. 在 Visual Studio（安装 SCOPE / Cosmos SDK）中打开 `scope_script.sln`
2. 打开 `dist_k_seg_finder_agg.script`
3. 将 `@RootPath` 调整为你有写权限的 VC 路径
4. 提交。完成后下载 TSV 输出文件

### MaxCompute / ODPS

1. 确保输入表 `raw_data(group_id BIGINT, x BIGINT)` 存在。脚本顶部的合成数据
   块可以为你填充它（取消注释即可）
2. 运行 `dist_k_seg_finder_agg.sql`。输出落入 `seg_result`、`seg_result_sort`、
   `seg_result_diff`

### 其它引擎（Spark SQL / Hive / Trino / BigQuery / ClickHouse）

MaxCompute 版本有意贴近 ANSI SQL。引擎适配通常只需：

- 将 `IF(cond, a, b)` 替换为 `CASE WHEN`（多数引擎也支持 `IF`）
- 将 `INSERT OVERWRITE TABLE … SELECT …` 替换为 `CREATE OR REPLACE TABLE … AS
  SELECT …` 或引擎原生等价语法
- 确认 floor 式取模 `(t % K + K) % K` 对你引擎的 `%` 运算符正确（多数引擎对
  `BIGINT` 是正确的）

---

## 验证块说明

`>>> VERIFY-BLOCK-BEGIN <<<` 和 `>>> VERIFY-BLOCK-END <<<` 标记之间的所有内容
仅用于交叉验证。在你的数据上确认聚合结果正确后，可以删除这些块；生产流水线只
保留 Step 0 → Step 1 → Step 2 和 `seg_result` 输出。

---

## 相关工作

本算法是已知构建块的组合，各组件均非全新发明：

- **Gaps-and-islands**（底层问题族）：Itzik Ben-Gan, *T-SQL Querying*, 2015。
  "Tabibitosan" 技巧（Aketi Jyuuzou, Oracle 社区）是经典排序方案。
- **复制 / 扇出 join**：Afrati & Ullman, *Optimizing Joins in a Map-Reduce
  Environment*, EDBT 2010。
- **分窗聚合**：Li, Maier, Tufte 等, *No Pane, No Gain*, SIGMOD Record 2005。
- **Halo / ghost cells**（模板计算 / HPC）—— 偏移指示器本质上是同一思路。
- **MapReduce 上的分布式连通分量**：Kiveris 等, *Connected Components in
  MapReduce and Beyond*, SOCC 2014。一维 CC 的特化就是我们在做的事情，`K`-桶
  利用线性结构跳过了迭代式指针跳跃。

据我所知没有已发表的名称对应这一组合；如需在文档或 PR 中引用，本仓库使用
**"Halo-Encoded Bucket Aggregation for Run Detection"**（又称
**Sortless Islands**）。

---

## 协议

MIT。详见仓库根目录。
