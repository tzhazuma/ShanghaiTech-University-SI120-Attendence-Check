# SI120 签到匹配工具

这个仓库里的 [attendance_matcher.py](attendance_matcher.py) 用来把名单、四轮密码表和签到结果自动匹配成最终得分表。

这个公开仓库只提交代码、测试、依赖说明和文档；真实名单、签到结果、密码表以及所有本地生成产物都通过 `.gitignore` 排除，不随仓库一起发布。下面文档里提到的 `name.xlsx`、`passwdN.xlsx`、`result.xlsx` 等名称都只是约定示例文件名。

现在整个项目已经扩展成一套完整工具链：

- [attendance_matcher.py](attendance_matcher.py)：签到结果匹配、评分、异常汇总
- [password_generator.py](password_generator.py)：生成下一轮密码，支持自动发现和并行生成
- [generate_next_passwd.py](generate_next_passwd.py)：最短入口，直接生成下一轮密码
- [check_passwords.py](check_passwords.py)：检查目标密码表与名单、历史密码的距离和一致性
- [check_latest_passwd.py](check_latest_passwd.py)：最短入口，直接检查当前目录中最新的密码表
- [tests/test_password_generator.py](tests/test_password_generator.py)：生成器和检查器的自动化测试

快速导航：

- [1. 匹配规则](#1-匹配规则)
- [3. 时间窗口模式](#3-时间窗口模式)
- [4. 输出文件](#4-输出文件)
- [5. 日志级别](#5-日志级别)
- [6. 典型命令](#6-典型命令)
- [12. 新密码生成](#12-新密码生成)
- [13. 密码检查](#13-密码检查)
- [14. 项目总结](#14-项目总结)

它支持下面这些能力：

- 模糊密码匹配
- 重复密码判零
- 名单与签到结果自动对齐
- Excel / CSV 双输入输出
- 官方固定窗口、手动窗口、配置文件窗口、默认启用的自适应窗口
- 多进程并行匹配
- 详细日志和异常汇总输出

## 1. 匹配规则

### 1.1 人员匹配

- 优先按学号匹配名单。
- 如果学号和姓名填反了，会自动识别并纠正。
- 如果学号无法匹配，但姓名在名单里唯一，也会回退到按姓名匹配。
- 如果仍然无法唯一定位到名单中的学生，则该条记录记为 `unresolved_student`。

### 1.2 密码模糊匹配

密码在匹配前会先做归一化：

- 忽略大小写
- `i` / `I` / `l` / `L` 都按 `1` 处理
- `o` / `O` 都按 `0` 处理
- 忽略密码中的空白字符

归一化之后，再做编辑距离匹配。

- 默认允许最多 `2` 个编辑错误
- 如果一条提交在阈值内只命中一个候选密码，则认为匹配成功
- 如果一条提交在阈值内命中多个候选密码，则记为 `ambiguous_password`
- 如果一条提交在阈值内一个候选密码都命不中，则记为 `unmatched_password`

### 1.3 重复密码判零

每一轮里，如果多个不同学生命中了同一个“规范密码”，那么这些学生这一轮都记 `0` 分。

这里按“匹配后的规范密码”判重，不按原始输入字符串判重。也就是说：

- `oHp6GZL0Cgs4`
- `oHp6GZLOCgs4`

会被看成同一个密码。

## 2. 输入文件要求

### 2.1 名单文件

支持 Excel 或 CSV。

至少要能识别出下面三列：

- 学号
- 姓名
- 邮箱

当前这批数据实际列名是：

- `学号`
- `姓名`
- `邮箱`

### 2.2 密码文件

支持 Excel 或 CSV。

每轮一份文件，至少要能识别出密码列：

- `password`
或
- `密码`

当前这批数据的密码表列名是：

- `学号`
- `姓名`
- `邮箱`
- `password`

### 2.3 签到结果文件

支持 Excel 或 CSV。

至少要能识别出下面三类信息：

- 提交时间
- 学号
- 姓名
- 密码

当前这批数据实际列名是：

- `提交答卷时间`
- `1、你的学号是`
- `2、你的名字是`
- `3、请输入发放的签到密码`

## 3. 时间窗口模式

脚本支持 4 种时间窗口模式。

### 3.1 adaptive

默认模式。

脚本会根据签到结果里的“活跃提交日期”自动划分为 4 个窗口。适合你已经知道有 4 轮，但不想手写日期的情况。

如果数据太少，无法稳定切成 4 个窗口，会自动回退到 `official`。

### 3.2 official

使用内置固定窗口。当前内置规则是：

- 第 1 次：从当年 `01-01` 到 `03-26`
- 第 2 次：`03-27` 到 `03-31`
- 第 3 次：`04-01` 到 `04-03`
- 第 4 次：从 `04-15` 开始

### 3.3 manual

在命令行里直接写 4 个时间窗口。

格式：

```text
--time-window START,END[,LABEL]
```

说明：

- `START` 必填
- `END` 必填，可以写 `open`
- `LABEL` 可选，不写时默认是“第1次签到”这种标签
- 如果只写日期，结束时间会自动扩成当天 `23:59:59.999999`

### 3.4 file

从配置文件读取时间窗口。

使用参数：

```text
--window-mode file --window-config-file PATH
```

支持：

- JSON
- CSV
- Excel

#### JSON 示例

```json
[
  {"start": "2026-01-01", "end": "2026-03-26", "label": "第1次签到"},
  {"start": "2026-03-27", "end": "2026-03-31", "label": "第2次签到"},
  {"start": "2026-04-01", "end": "2026-04-03", "label": "第3次签到"},
  {"start": "2026-04-15", "end": "open", "label": "第4次签到"}
]
```

也支持这种包一层的格式：

```json
{
  "windows": [
    {"start": "2026-01-01", "end": "2026-03-26", "label": "第1次签到"},
    {"start": "2026-03-27", "end": "2026-03-31", "label": "第2次签到"},
    {"start": "2026-04-01", "end": "2026-04-03", "label": "第3次签到"},
    {"start": "2026-04-15", "end": "open", "label": "第4次签到"}
  ]
}
```

#### CSV 示例

```csv
start,end,label
2026-01-01,2026-03-26,第1次签到
2026-03-27,2026-03-31,第2次签到
2026-04-01,2026-04-03,第3次签到
2026-04-15,open,第4次签到
```

CSV 或 Excel 中至少要有：

- `start`
- `end`

可选列：

- `label`

中文列名 `开始`、`结束`、`标签` 也支持。

## 4. 输出文件

### 4.1 Excel 输出

如果输出为 `.xlsx`，会生成一个工作簿，包含这些 sheet：

- `scores`
- `details`
- `mapping`
- `exceptions`

各 sheet 含义：

- `scores`：按名单顺序输出每个人每一轮的得分和总分
- `details`：每一条提交的详细判定结果
- `mapping`：时间窗口和密码文件映射推断结果
- `exceptions`：异常汇总表，汇总 `duplicate_password`、`unmatched_password` 等异常类型

### 4.2 CSV 输出

如果输出格式是 CSV，会生成 4 个文件：

- 主结果：`xxx.csv`
- 明细：`xxx_details.csv`
- 映射：`xxx_mapping.csv`
- 异常汇总：`xxx_exceptions.csv`

## 5. 日志级别

使用参数：

```text
--log-level quiet|info|debug
```

说明：

- `quiet`：只输出警告和必要结果
- `info`：输出主要推断信息，适合日常使用
- `debug`：输出详细过程，适合排查自适应窗口和密码映射问题

`debug` 模式下会额外打印：

- 自适应窗口活跃日期
- 日期间隔与切分点
- 每个窗口对每个密码文件的命中数
- 密码映射候选总分 Top 5

## 6. 典型命令

### 6.1 默认自适应窗口，输出 Excel

```bash
cd /home/azuma/si120
./.venv/bin/python attendance_matcher.py \
  --name-file name.xlsx \
  --password-files passwd1.xlsx passwd2.xlsx passwd3.xlsx passwd4.xlsx \
  --result-file si120attendence_result_1to4.xlsx \
  --output-file attendance_scores.xlsx
```

### 6.2 默认自适应窗口，输出 CSV

```bash
cd /home/azuma/si120
./.venv/bin/python attendance_matcher.py \
  --name-file name.xlsx \
  --password-files passwd1.xlsx passwd2.xlsx passwd3.xlsx passwd4.xlsx \
  --result-file si120attendence_result_1to4.xlsx \
  --output-file attendance_scores \
  --output-format csv
```

### 6.3 使用 fixed official 窗口

```bash
cd /home/azuma/si120
./.venv/bin/python attendance_matcher.py \
  --name-file name.xlsx \
  --password-files passwd1.xlsx passwd2.xlsx passwd3.xlsx passwd4.xlsx \
  --result-file si120attendence_result_1to4.xlsx \
  --output-file attendance_scores_official.xlsx \
  --window-mode official
```

### 6.4 手动配置时间窗口

```bash
cd /home/azuma/si120
./.venv/bin/python attendance_matcher.py \
  --name-file name.xlsx \
  --password-files passwd1.xlsx passwd2.xlsx passwd3.xlsx passwd4.xlsx \
  --result-file si120attendence_result_1to4.xlsx \
  --output-file attendance_scores_manual.xlsx \
  --window-mode manual \
  --time-window 2026-01-01,2026-03-26,第1次签到 \
  --time-window 2026-03-27,2026-03-31,第2次签到 \
  --time-window 2026-04-01,2026-04-03,第3次签到 \
  --time-window 2026-04-15,open,第4次签到
```

### 6.5 从 JSON 窗口配置文件读取

```bash
cd /home/azuma/si120
./.venv/bin/python attendance_matcher.py \
  --name-file name.xlsx \
  --password-files passwd1.xlsx passwd2.xlsx passwd3.xlsx passwd4.xlsx \
  --result-file si120attendence_result_1to4.xlsx \
  --output-file attendance_scores_from_json.xlsx \
  --window-mode file \
  --window-config-file round_windows.json
```

### 6.6 从 CSV 窗口配置文件读取

```bash
cd /home/azuma/si120
./.venv/bin/python attendance_matcher.py \
  --name-file name.xlsx \
  --password-files passwd1.xlsx passwd2.xlsx passwd3.xlsx passwd4.xlsx \
  --result-file si120attendence_result_1to4.xlsx \
  --output-file attendance_scores_from_csv_window.xlsx \
  --window-mode file \
  --window-config-file round_windows.csv
```

### 6.7 输出详细调试日志

```bash
cd /home/azuma/si120
./.venv/bin/python attendance_matcher.py \
  --name-file name.xlsx \
  --password-files passwd1.xlsx passwd2.xlsx passwd3.xlsx passwd4.xlsx \
  --result-file si120attendence_result_1to4.xlsx \
  --output-file attendance_scores_debug.xlsx \
  --log-level debug
```

## 7. 密码文件与轮次映射

脚本默认会自动推断“时间窗口 -> 密码文件”的最佳对应关系。

如果你已经明确知道对应关系，也可以手动指定：

```bash
--password-order 1 3 2 4
```

意思是：

- 第 1 个窗口对应第 1 个密码文件
- 第 2 个窗口对应第 3 个密码文件
- 第 3 个窗口对应第 2 个密码文件
- 第 4 个窗口对应第 4 个密码文件

## 8. 依赖安装

当前依赖写在 [requirements.txt](requirements.txt)：

- `pandas`
- `openpyxl`

如果还没有虚拟环境，可以这样安装：

```bash
cd /home/azuma/si120
python3 -m venv .venv
./.venv/bin/python -m pip install -r requirements.txt
```

## 9. 本地验证示例

这些脚本已经在本地真实数据上验证过：

- 默认自适应窗口可正常划分 4 轮
- Excel 输出和 CSV 输出都可用
- 手动窗口和窗口配置文件模式都可用
- 异常汇总表会正确生成
- debug 日志会输出自适应切分和密码映射推断过程

其中一组本地数据的自适应窗口示例如下：

- `2026-03-18`
- `2026-03-27` 到 `2026-03-28`
- `2026-04-01` 到 `2026-04-02`
- `2026-04-15`

并且自动推断出的密码映射顺序是：

- 第 1 轮 -> 第 1 个密码文件
- 第 2 轮 -> 第 3 个密码文件
- 第 3 轮 -> 第 2 个密码文件
- 第 4 轮 -> 第 4 个密码文件

## 10. 常见问题

### 10.1 为什么有同学明明输得很像还是 0 分？

可能原因有：

- 这个密码在阈值 2 内仍然匹配不到任何候选密码
- 这个密码同时匹配到多个候选，变成 `ambiguous_password`
- 这个密码虽然匹配上了，但与其他学生撞了同一个规范密码，触发 `duplicate_password`
- 学号和姓名都无法正确定位到名单中的学生

### 10.2 为什么明细里是匹配成功，但最后还是 0 分？

最常见原因是：

- 匹配到了密码，但这个规范密码被多个不同学生共用，所以触发重复判零

### 10.3 自适应窗口什么时候不该用？

如果签到日期分布本身不清晰，比如：

- 中间有大量补交
- 多轮提交日期彼此交叠很严重
- 轮次数不是 4

这种情况更建议用：

- `--window-mode official`
或
- `--window-mode file`

## 11. 快速结论

日常最推荐的用法是：

- 默认自适应窗口
- 输出 xlsx
- 需要排查时打开 `--log-level debug`

如果你已经明确知道每轮时间范围，最稳妥的是：

- 用 `--window-mode file`
- 把窗口写到 JSON 或 CSV 文件里

## 12. 新密码生成

如果原先生成随机密码的代码找不到了，现在可以直接使用 [password_generator.py](password_generator.py) 生成新一轮密码。

如果你只想“无脑生成下一轮”，也可以直接运行 [generate_next_passwd.py](generate_next_passwd.py)。它会调用同一套生成和校验逻辑，但命令名更直接。

这个生成器会主动补上几个原始需求里容易遗漏但实际必须有的约束：

- 新密码默认与现有密码格式保持一致
- 新密码彼此唯一
- 新密码在模糊匹配规则下与前几轮密码保持安全距离
- 默认把实际使用的 seed 写入元数据文件，方便复现
- 生成过程支持并行

### 12.1 默认行为

如果你直接在当前目录运行生成器，它会：

- 优先读取 `name.xlsx` 作为名单；如果没有，再回退查找 `name.csv` 等合法名单文件
- 自动搜索当前目录下 `passwd` 相关的表格文件，并只把合法的 `passwdN` 文件当作参考密码文件
- 自动跳过不合法的 `passwd` 表格，并在日志里说明原因
- 从参考密码文件推断默认长度、字符集和安全距离
- 默认输出与现有密码表相同结构的文件：`学号 / 姓名 / 邮箱 / password`
- 默认输出一个发放版文件，只保留 `学号 / password`
- 默认发放版输出为纯 `CSV`，方便直接发给助教或导入其他系统
- 默认输出一个单独的元数据 JSON 文件，记录 seed、算法、长度、最小距离和重试统计
- 如果没有显式提供 `--output-file`，会根据当前目录中最大的 `passwdN` 自动生成 `passwd(N+1).xlsx`
- 如果显式提供 `--round 7` 且没有传 `--output-file`，会优先生成 `passwd7.xlsx`

### 12.2 可选参数

生成器支持下面这些核心参数：

- `--search-dir`：默认搜索目录
- `--name-file`：显式指定名单文件；不传时自动发现
- `--reference-files`：显式指定参考密码表
- `--output-file`：显式指定输出密码文件；不传时自动按最大 `passwdN + 1` 生成
- `--round`：显式指定目标轮次；不传时默认按最大 `passwdN + 1` 推断
- `--issue-file`：显式指定发放版文件路径
- `--issue-format`：发放版输出格式，默认 `csv`
- `--no-issue-file`：不生成发放版文件
- `--algorithm`：随机算法，可选 `blake2-counter`、`splitmix64`、`xorshift64star`
- `--seed`：自定义随机 seed；不传时自动生成安全 seed
- `--length`：自定义位数；不传时自动推断
- `--alphabet`：自定义字符集；不传时自动推断
- `--min-distance`：自定义最小归一化编辑距离；不传时自动推断
- `--workers`：并行进程数，默认全核
- `--max-attempts-per-row`：单个密码最多重试次数
- `--metadata-file`：元数据 JSON 输出路径

### 12.3 随机算法说明

#### `blake2-counter`

- 默认算法
- 基于哈希计数器
- 并行安全，和任务调度顺序无关
- 同一个 seed、同一行、同一次尝试，总会生成相同密码

#### `splitmix64`

- 自定义 64 位伪随机序列
- 速度快
- 适合需要稳定可复现的场景

#### `xorshift64star`

- 自定义 64 位 xorshift 变体
- 速度也很快
- 同样支持 seed 和并行复现

### 12.4 典型命令

#### 最简单的一键生成下一轮密码

```bash
cd /home/azuma/si120
./.venv/bin/python generate_next_passwd.py
```

这条命令会自动：

- 选用默认 `name.xlsx`
- 自动搜索合法的 `passwdN` 参考文件
- 自动生成下一个输出名，例如当前已有 `passwd1` 到 `passwd5` 时，就输出 `passwd6.xlsx`
- 同时生成 `passwd6_issue.csv` 和 `passwd6_metadata.json`

#### 使用主生成器零参数生成下一轮密码

```bash
cd /home/azuma/si120
./.venv/bin/python password_generator.py
```

#### 生成第 5 次签到密码，输出 Excel

```bash
cd /home/azuma/si120
./.venv/bin/python password_generator.py \
  --name-file name.xlsx \
  --reference-files passwd1.xlsx passwd2.xlsx passwd3.xlsx passwd4.xlsx \
  --output-file passwd5.xlsx
```

#### 显式指定要生成第 7 轮，并使用默认命名

```bash
cd /home/azuma/si120
./.venv/bin/python password_generator.py \
  --round 7
```

这条命令在未传 `--output-file` 时会默认输出：

- `passwd7.xlsx`
- `passwd7_issue.csv`
- `passwd7_metadata.json`

#### 生成第 5 次签到密码，并显式指定 seed

```bash
cd /home/azuma/si120
./.venv/bin/python password_generator.py \
  --name-file name.xlsx \
  --reference-files passwd1.xlsx passwd2.xlsx passwd3.xlsx passwd4.xlsx \
  --output-file passwd5.xlsx \
  --seed si120-round5-2026-04-16
```

#### 强制指定位数、字符集和最小距离

```bash
cd /home/azuma/si120
./.venv/bin/python password_generator.py \
  --name-file name.xlsx \
  --reference-files passwd1.xlsx passwd2.xlsx passwd3.xlsx passwd4.xlsx \
  --output-file passwd5.xlsx \
  --length 12 \
  --alphabet 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz \
  --min-distance 6
```

#### 切换随机算法并输出详细日志

```bash
cd /home/azuma/si120
./.venv/bin/python password_generator.py \
  --name-file name.xlsx \
  --reference-files passwd1.xlsx passwd2.xlsx passwd3.xlsx passwd4.xlsx \
  --output-file passwd5.xlsx \
  --algorithm splitmix64 \
  --log-level debug
```

### 12.5 输出文件

生成器默认会输出三类文件：

- 密码表，例如 `passwd5.xlsx`
- 发放版文件，例如 `passwd5_issue.csv`
- 元数据文件，例如 `passwd5_metadata.json`

发放版文件只保留：

- `学号`
- `password`

元数据里会记录：

- 实际使用的 seed
- 随机算法
- 目标轮次
- 密码长度
- 字符集
- 最小归一化编辑距离
- 参考文件列表
- 哪些参考 `passwd` 文件被自动跳过以及原因
- 是否自动发现了名单、参考文件和输出文件名
- 重试次数和最终最小距离
- 与参考密码最近的若干样例
- 新生成密码内部彼此最近的若干样例

### 12.6 测试

生成器对应的自动化测试在 [tests/test_password_generator.py](tests/test_password_generator.py)。

这些测试会覆盖：

- 同一 seed 下的确定性
- 与参考密码及新生成密码之间的距离约束
- CLI 输出与元数据写出
- 默认发现 `name.xlsx`
- 自动选择下一个 `passwdN`
- 自动发放版输出
- 自动跳过不合法参考密码文件

可以这样运行：

```bash
cd /home/azuma/si120
./.venv/bin/python -m unittest tests/test_password_generator.py -v
```

## 13. 密码检查

如果你想在生成完某一轮密码之后，再做一次结构和距离检查，可以使用 [check_passwords.py](check_passwords.py)。

它主要检查这些内容：

- 目标密码表是否能正常识别出 `学号` 和 `password`
- 目标密码表与名单是否对齐，有没有缺失学号、额外学号、重复学号
- 密码是否有原始重复和归一化后重复
- 与历史参考密码最接近的样例有哪些
- 新密码内部彼此最接近的样例有哪些

### 13.1 默认行为

直接运行时，它会：

- 自动发现 `name.xlsx` 或其他合法名单文件
- 自动使用当前目录中最新的合法 `passwdN` 作为待检查目标
- 自动使用其余合法 `passwdN` 作为参考密码表
- 默认输出 `passwdN_check.xlsx`

### 13.2 最简单的检查命令

```bash
cd /home/azuma/si120
./.venv/bin/python check_latest_passwd.py
```

或者：

```bash
cd /home/azuma/si120
./.venv/bin/python check_passwords.py
```

### 13.3 指定目标文件检查

```bash
cd /home/azuma/si120
./.venv/bin/python check_passwords.py \
  --target-file passwdN.xlsx \
  --reference-files passwd1.xlsx passwd2.xlsx passwd3.xlsx passwd4.xlsx passwd5.xlsx
```

### 13.4 检查报告输出

默认输出为一个 Excel 工作簿，包含这些 sheet：

- `summary`
- `student_issues`
- `password_duplicates`
- `closest_reference`
- `closest_internal`
- `target_profile`

如果你传 `--output-format csv`，则会拆成多份 CSV 文件。

## 14. 项目总结

现在这个目录里的脚本已经覆盖了整个签到密码工作流：

### 14.1 历史签到统计

用 [attendance_matcher.py](attendance_matcher.py) 读取：

- 名单
- 四轮密码文件
- 签到结果问卷

然后自动完成：

- 时间窗口划分
- 密码文件与轮次映射
- 模糊匹配
- 重复判零
- 得分汇总和异常导出

### 14.2 新一轮密码生成

用 [password_generator.py](password_generator.py) 或 [generate_next_passwd.py](generate_next_passwd.py) 完成：

- 自动发现名单与历史密码表
- 推断长度、字符集和安全距离
- 并行生成新密码
- 导出完整密码表、发放版 CSV 和元数据 JSON

### 14.3 生成后审计

用 [check_passwords.py](check_passwords.py) 或 [check_latest_passwd.py](check_latest_passwd.py) 完成：

- 名单一致性检查
- 重复检查
- 与历史密码距离检查
- 新密码内部距离检查

### 14.4 推荐日常流程

如果后面还要继续维护这个项目，最推荐的顺序是：

1. 先用 [generate_next_passwd.py](generate_next_passwd.py) 生成下一轮密码。
2. 再用 [check_latest_passwd.py](check_latest_passwd.py) 检查最新密码表。
3. 课堂结束后用 [attendance_matcher.py](attendance_matcher.py) 汇总签到和得分。

这样三个脚本分别负责“生成、检查、统计”，职责比较清晰，也方便后续继续扩展。
