# 定义源文件路径和目标目录
$sourceFile = "e:\backup\old\alarm.csv"
$destinationDir = "\\10.31.7.16\logs_backup\"

# 检查源文件是否存在
if (-not (Test-Path $sourceFile)) {
    Write-Host "源文件不存在: $sourceFile" -ForegroundColor Red
    exit 1
}

# 检查目标网络共享是否可用
if (-not (Test-Path $destinationDir)) {
    Write-Host "网络路径不可访问: $destinationDir" -ForegroundColor Red
    Write-Host "请检查网络连接或共享权限"
    exit 1
}

# 生成带时间戳的新文件名（防止覆盖）
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$newFileName = "backup_$timestamp.log"
$destinationPath = Join-Path $destinationDir $newFileName

try {
    # 移动文件并重命名
    Move-Item -Path $sourceFile -Destination $destinationPath -Force -ErrorAction Stop
    Write-Host "文件已成功移动到: $destinationPath" -ForegroundColor Green
}
catch {
    Write-Host "移动文件时出错: $_" -ForegroundColor Red
    exit 1
}