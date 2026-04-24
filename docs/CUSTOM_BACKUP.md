# 自定义内容备份与回灌

当你需要重新下载上游仓库，或者担心后续同步时误伤自己的理论、策略和扩展代码时，可以先把自定义内容独立打包备份。

## 备份范围

默认清单在：

- `custom_assets.manifest.json`

当前主要覆盖：

- `src/models/` 下的自定义研究模型
- `strategies/` 下的新增策略 YAML
- `src/daily_push_pipeline.py`
- `src/market_data_fetcher.py`
- `send_email.py`
- `docs/CUSTOM_EXTENSIONS.md`
- `docs/ASSET_OPERATIONS.md`
- 对应测试文件

后续新增自定义内容时，只要把路径或 glob 追加到 `custom_assets.manifest.json` 即可。

## 常用命令

先查看哪些文件会被纳入备份：

```bash
python3 scripts/backup_custom_assets.py list
```

创建备份包：

```bash
python3 scripts/backup_custom_assets.py backup
```

默认会生成到：

- `backups/custom-assets-<UTC时间戳>.tar.gz`

## 回灌到新下载的仓库

先做只读预检查，不直接写文件：

```bash
python3 scripts/backup_custom_assets.py restore backups/custom-assets-20260424-000000.tar.gz --target-repo /path/to/new-repo --dry-run
```

输出会分成三类：

- `[new]`：新仓库里还没有的文件
- `[changed]`：新仓库里已存在但内容不同，属于需要你审核的潜在冲突
- `[unchanged]`：内容已一致，不需要恢复

确认无误后再真正恢复：

```bash
python3 scripts/backup_custom_assets.py restore backups/custom-assets-20260424-000000.tar.gz --target-repo /path/to/new-repo --overwrite
```

## 推荐流程

1. 先在当前仓库执行 `backup`
2. 重新下载或更新上游仓库
3. 先执行 `restore --dry-run`
4. 先看文字冲突清单，再决定是否恢复
5. 审核后再执行 `restore --overwrite`

这样就算以后你重新拉取上游代码，也能把自己的理论和定制内容重新接回去，而且不会一上来就盲目覆盖。

## GitHub 云端备份

仓库内已新增备份工作流：

- `.github/workflows/custom-assets-backup.yml`

使用方式：

1. 打开 GitHub 仓库的 `Actions`
2. 选择 `Custom Assets Backup`
3. 可手动运行，也会在每周一 UTC 03:00 自动运行
4. 运行完成后，在该次 workflow 的 `Artifacts` 中下载：
   - `custom-assets-files-<run_number>`
   - `custom-assets-backup-<run_number>`

这样即使你本地环境出了问题，也可以先重新下载上游仓库，再从 GitHub artifact 里取回你自己的备份包。

## 上游更新前提醒

仓库内的 `Upstream Update Check` 工作流现在会在检测到上游有新提交时，自动在运行摘要和跟踪 issue 里提醒你先备份。

推荐顺序：

1. 先运行 `Custom Assets Backup` 或本地执行 `python3 scripts/backup_custom_assets.py backup`
2. 再检查和处理上游更新
3. 需要重新接回自定义内容时，先执行 `restore --dry-run`
