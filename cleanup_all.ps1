Write-Host "=== Cong cu don dep sau he thong (System Cleanup Tool) ===" -ForegroundColor Cyan

# 1. Don dep Temp Folder (Thu pham chinh 168GB)
$tempPath = $env:TEMP
Write-Host "1. Dang quet va xoa file rac trong Temp ($tempPath)..." -ForegroundColor Yellow
Write-Host "Luu y: Mot so file dang duoc su dung boi he thong se khong the xoa (se co thong bao loi mau do, hay bo qua chung)." -ForegroundColor Gray

try {
    # Delete files older than 24 hours to be safe, or just force all? 
    # Force all is usually fine for temp, but let's be slightly gentle and do recursive force.
    Get-ChildItem -Path $tempPath -Recurse -Force -ErrorAction SilentlyContinue | Where-Object { 
        # Skip files locked by processes
        $_.LastWriteTime -lt (Get-Date).AddMinutes(-5) 
    } | Remove-Item -Force -Recurse -ErrorAction SilentlyContinue
} catch {
    Write-Host "Co loi khi xoa mot so file (dieu nay binh thuong voi Temp)" -ForegroundColor Gray
}
Write-Host "-> Da don dep xong thu muc Temp." -ForegroundColor Green

# 2. Xu ly Ubuntu WSL Disk (16GB) - Neu co
$ubuntuDisk = "$env:LOCALAPPDATA\Packages\CanonicalGroupLimited.Ubuntu_79rhkp1fndgsc\LocalState\ext4.vhdx"
if (Test-Path $ubuntuDisk) {
    Write-Host "`n2. Phat hien Ubuntu WSL Disk (~16GB). Dang chuan bi co gon..." -ForegroundColor Yellow
    
    # Tat WSL
    wsl --shutdown
    Start-Sleep -Seconds 3

    # Kiem tra quyen Admin
    if (-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
        Write-Host "Warning: Ban can chay voi quyen Admin de co gon file Ubuntu nay. Script se bo qua buoc nay." -ForegroundColor Red
    } else {
         # Tao script diskpart
        $dpScript = "diskpart_ubuntu.txt"
        $commands = @"
select vdisk file="$ubuntuDisk"
attach vdisk readonly
compact vdisk
detach vdisk
"@
        $commands | Out-File -FilePath $dpScript -Encoding ASCII
        
        Write-Host "Dang chay Diskpart len Ubuntu Disk... (Mat khoang 1-2 phut)" -ForegroundColor Yellow
        diskpart /s $dpScript | Out-Null
        Remove-Item $dpScript -Force
        Write-Host "-> Da co gon xong Ubuntu Disk." -ForegroundColor Green
    }
}

# 3. Cache NPM (1GB)
$npmCache = "$env:LOCALAPPDATA\npm-cache"
if (Test-Path $npmCache) {
    Write-Host "`n3. Xoa Cache NPM..." -ForegroundColor Yellow
    Remove-Item -Path $npmCache -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "-> Da xoa Cache NPM." -ForegroundColor Green
}

# 4. Cache Pip (0.5GB)
$pipCache = "$env:LOCALAPPDATA\pip\cache"
if (Test-Path $pipCache) {
    Write-Host "`n4. Xoa Cache Pip..." -ForegroundColor Yellow
    Remove-Item -Path $pipCache -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "-> Da xoa Cache Pip." -ForegroundColor Green
}

Write-Host "`n=== HOAN TAT QUA TRINH DON DEP ===" -ForegroundColor Cyan
Write-Host "Hay kiem tra lai dung luong o C cua ban."
Read-Host "Nhan Enter de thoat..."