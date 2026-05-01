#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
邮件推送脚本 - 在本地电脑运行，将分析报告发送到指定邮箱

使用方法：
    1. 确保已安装 Python 3
    2. 运行：python send_email.py
    3. 脚本会自动读取 reports/ 目录下的最新报告并发送邮件

依赖：无需额外安装，使用 Python 标准库 smtplib
"""

import smtplib
import os
import glob
import sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr
from datetime import datetime

# ============ 配置（按需修改） ============
SENDER = "906249822@qq.com"
PASSWORD = "ypuhhrdqksiabdbg"  # QQ邮箱SMTP授权码
RECEIVERS = ["934197233@qq.com"]
SMTP_SERVER = "smtp.qq.com"
SMTP_PORT = 465
# =========================================

# 报告目录（与脚本同目录下的 reports 文件夹）
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(SCRIPT_DIR, "reports")


def send_email(subject: str, content: str, receivers: list = None) -> bool:
    """发送邮件"""
    receivers = receivers or RECEIVERS

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"] = formataddr((str(Header("股票分析助手", "utf-8")), SENDER))
        msg["To"] = ", ".join(receivers)

        # 纯文本版本
        msg.attach(MIMEText(content, "plain", "utf-8"))

        # 简单HTML版本
        html = content.replace("\n", "<br>").replace("**", "<b>").replace("**", "</b>")
        msg.attach(MIMEText(f"<html><body><pre style='font-size:14px'>{html}</pre></body></html>", "html", "utf-8"))

        # SSL连接
        server = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, timeout=30)
        server.login(SENDER, PASSWORD)
        server.send_message(msg)
        server.quit()

        print(f"✅ 邮件发送成功！收件人: {receivers}")
        return True

    except smtplib.SMTPAuthenticationError:
        print("❌ 认证失败：请检查邮箱和授权码是否正确")
        return False
    except Exception as e:
        print(f"❌ 发送失败: {e}")
        return False


def find_latest_reports() -> list:
    """查找最新的报告文件"""
    if not os.path.exists(REPORTS_DIR):
        print(f"⚠️ 报告目录不存在: {REPORTS_DIR}")
        return []

    # 查找今日报告
    today = datetime.now().strftime("%Y%m%d")
    patterns = [
        os.path.join(REPORTS_DIR, f"*_{today}.md"),
        os.path.join(REPORTS_DIR, f"stock_analysis_{today}.md"),
        os.path.join(REPORTS_DIR, "market_review_*.md"),
    ]

    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern))

    # 按修改时间排序，取最新的
    files.sort(key=lambda f: os.path.getmtime(f), reverse=True)
    return files[:10]  # 最多取10个文件


def main():
    print("=" * 50)
    print("📧 股票分析报告 - 邮件推送工具")
    print("=" * 50)

    # 检查配置
    if not SENDER or not PASSWORD:
        print("❌ 请先配置 SENDER 和 PASSWORD")
        sys.exit(1)

    # 查找报告
    reports = find_latest_reports()

    if not reports:
        print("⚠️ 未找到今日报告文件")
        print(f"   报告目录: {REPORTS_DIR}")

        # 尝试查找所有 md 文件
        all_md = glob.glob(os.path.join(REPORTS_DIR, "*.md"))
        if all_md:
            all_md.sort(key=lambda f: os.path.getmtime(f), reverse=True)
            print(f"   找到 {len(all_md)} 个历史报告，将发送最新的")
            reports = all_md[:5]
        else:
            print("   没有任何报告文件可发送")
            sys.exit(1)

    print(f"\n找到 {len(reports)} 个报告文件：")
    for f in reports:
        mtime = datetime.fromtimestamp(os.path.getmtime(f))
        print(f"  📄 {os.path.basename(f)} ({mtime.strftime('%Y-%m-%d %H:%M')})")

    # 合并报告内容
    all_content = []
    for filepath in reports:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()
                all_content.append(f"## {os.path.basename(filepath)}\n\n{content}")
        except Exception as e:
            print(f"⚠️ 读取 {filepath} 失败: {e}")

    if not all_content:
        print("❌ 没有可发送的内容")
        sys.exit(1)

    combined = "\n\n---\n\n".join(all_content)
    date_str = datetime.now().strftime("%Y-%m-%d")
    subject = f"📈 股票智能分析报告 - {date_str}"

    print(f"\n正在发送邮件...")
    success = send_email(subject, combined)

    if success:
        print(f"\n✅ 推送完成！共发送 {len(reports)} 个报告")
    else:
        print(f"\n❌ 推送失败，请检查网络和配置")
        sys.exit(1)


if __name__ == "__main__":
    main()
