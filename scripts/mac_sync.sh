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
echo ""
echo "    ⚠️ 请在下方命令中填入正确的 skill_id 和参数。"
echo "    如果你不确定，可以把 ttskill 支持的命令贴给我，我帮你拼接。"
echo ""
echo "    参考命令:"
echo '    ttskill signed-invoke \'
echo '      --skill-id FUND_HOLDINGS \'
echo '      --data '"'"'{}'"'"' \'
echo "      > $JSON_FILE"
echo ""

# Step 2b: 如果 JSON 文件已存在（手动放置），跳过拉取
if [ -f "$JSON_FILE" ]; then
    echo "📄 检测到已有持仓文件: $JSON_FILE"
    echo "   如需重新拉取，请删除此文件后重试。"
else
    echo "   请在终端中运行 ttskill 命令，将输出保存到:"
    echo "   $JSON_FILE"
    echo ""
    echo "   完成后按 Enter 继续..."
    read -r
fi

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
