Start-Transcript -Path "e:\script\Logs\MoveBackupLog.log" -Append

# 定义源文件路径和目标目录
$sourceFile = "e:\backup\alarm.csv"
$destinationDir = "\\10.31.9.24\Datacollector\IT\scada_alarm\"

# 检查源文件是否存在
if (-not (Test-Path $sourceFile)) {
    Write-Host "源文件不存在: $sourceFile" -ForegroundColor Red
    exit 1
}

# 检查目标网络共享是否可用
if (-not (Test-Path $destinationDir)) {
    Write-Host "network not access: $destinationDir" -ForegroundColor Red
    Write-Host "please check network"
    exit 1
}

# 生成带时间戳的新文件名（防止覆盖）
$timestamp = (Get-Date).AddDays(-1).ToString("yyyyMM")
$newFileName = "SATalarm_$timestamp.csv"
$destinationPath = Join-Path $destinationDir $newFileName

try {
    # 移动文件并重命名
	Copy-Item -Path $sourceFile -Destination "E:\backup\old\$newFileName" -Force -ErrorAction Stop
    Move-Item -Path $sourceFile -Destination $destinationPath -Force -ErrorAction Stop
    Write-Host "file move success: $destinationPath" -ForegroundColor Green
}
catch {
    Write-Host "move file error: $_" -ForegroundColor Red
    exit 1
}