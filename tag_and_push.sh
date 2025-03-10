#!/bin/bash

# 检查是否提供了版本号
if [ -z "$1" ]; then
    echo "错误: 版本号未提供。"
    echo "用法: ./tag_and_push.sh <版本号>"
    exit 1
fi

# 获取版本号
version=$1

# 确认当前是否在 Git 仓库中
if ! git rev-parse --is-inside-work-tree > /dev/null 2>&1; then
    echo "错误: 当前目录不是 Git 仓库。"
    exit 1
fi

# 检查是否有未提交的更改
if ! git diff-index --quiet HEAD --; then
    echo "警告: 存在未提交的更改，请先提交再打标签。"
    exit 1
fi

# 创建 Git 标签
echo "正在创建 Git 标签: v$version"
git tag "v$version"

# 检查标签是否创建成功
if [ $? -ne 0 ]; then
    echo "错误: Git 标签创建失败。"
    exit 1
fi

# 推送标签到远程仓库
echo "正在推送标签到远程仓库..."
git push origin "v$version"

# 检查推送是否成功
if [ $? -eq 0 ]; then
    echo "成功: 标签 v$version 已推送到远程仓库。"
else
    echo "错误: 推送标签失败。"
    exit 1
fi