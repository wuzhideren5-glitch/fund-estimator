#!/bin/bash
# =============================================================
# Mac 端持仓同步脚本
# 用法: ./mac_sync.sh
# 
# 前提: 
#   1. ttskill 已安装 (https://skills.tiantianfunds.com)
#   2. ttskill login 已完成
#   3. 本脚本放在 fund-estimator 仓库根目录
# =============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
JSON_FILE="/tmp/holdings_$(date +%Y%m%d).json"
DATE_STR=$(date '+%Y-%m-%d %H:%M')

echo "🔄 天天基金持仓同步 — $DATE_STR"
echo "========================================"

# Step 1: 检查 ttskill
if ! command -v ttskill &> /dev/null; then
    echo "❌ ttskill 未安装。请先安装: https://skills.tiantianfunds.com"
    exit 1
fi

# Step 2: 从天天基金拉取持仓
echo "📥 拉取最新持仓..."
ttskill invoke ACCOUNT_HOLDING --action holding_list --body '{}' > "$JSON_FILE"
echo "✅ 持仓数据已保存到 $JSON_FILE"

# Step 3: 运行同步脚本
if [ ! -f "$JSON_FILE" ]; then
    echo "❌ 持仓文件不存在: $JSON_FILE"
    echo "   请先运行 ttskill 命令导出持仓数据。"
    exit 1
fi

echo "🔧 转换持仓数据..."
python3 "$SCRIPT_DIR/sync_from_json.py" "$JSON_FILE"

# Step 4: Git 提交并推送
echo ""
echo "📤 推送到 GitHub..."
cd "$REPO_DIR"
git add backend/import_holdings.py
git commit -m "🔄 同步持仓 $(date +%Y-%m-%d)" || echo "   (无变更，跳过 commit)"
git push origin main

echo ""
echo "✅ 同步完成！服务器将在下次定时任务中自动拉取。"
