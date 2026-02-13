Write-Host "=== Phat hien va xu ly file ao hoa Docker chiem dung luong ===" -ForegroundColor Cyan

# 1. Tat WSL va Docker Desktop de nha file
Write-Host "1. Dang tat WSL va Docker Desktop..." -ForegroundColor Yellow
wsl --shutdown
Start-Sleep -Seconds 5

# 2. Xac dinh duong dan file VHDX (dua tren ket qua quet cua t)
$targetPath = "$env:USERPROFILE\AppData\Local\Docker\wsl\disk\docker_data.vhdx"

if (-not (Test-Path $targetPath)) {
    Write-Host "Khong tim thay file tai: $targetPath" -ForegroundColor Red
    Write-Host "Thu tim o vi tri mac dinh khac..."
    $targetPath = "$env:USERPROFILE\AppData\Local\Docker\wsl\data\ext4.vhdx"
}

if (Test-Path $targetPath) {
    if (-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
        Write-Host "CANH BAO: Ban can chay script nay voi quyen Admin (Run as Administrator) de lenh diskpart hoat dong!" -ForegroundColor Red
        Write-Warning "Vui long chuot phai vao file -> Run with PowerShell (Run as Administrator)"
        Pause
        exit
    }

    $sizeBefore = (Get-Item $targetPath).Length / 1GB
    Write-Host "Tim thay file VHDX tai: $targetPath" -ForegroundColor Green
    Write-Host "Kich thuoc hien tai: $("0:N2" -f $sizeBefore) GB" -ForegroundColor Magenta
    
    # 3. Tao script cho Diskpart
    $dpScript = "diskpart_cmds.txt"
    $commands = @"
select vdisk file="$targetPath"
attach vdisk readonly
compact vdisk
detach vdisk
"@
    $commands | Out-File -FilePath $dpScript -Encoding ASCII

    # 4. Chay Diskpart
    Write-Host "2. Dang thuc hien co gon (Compact) file dia... Vui long doi..." -ForegroundColor Yellow
    diskpart /s $dpScript

    # 5. Don dep
    Remove-Item $dpScript -Force
    
    $sizeAfter = (Get-Item $targetPath).Length / 1GB
    $reclaimed = $sizeBefore - $sizeAfter
    
    Write-Host "=== HOAN TAT ===" -ForegroundColor Cyan
    Write-Host "Kich thuoc truoc: $("0:N2" -f $sizeBefore) GB"
    Write-Host "Kich thuoc sau:   $("0:N2" -f $sizeAfter) GB"
    Write-Host "Da lay lai duoc:  $("0:N2" -f $reclaimed) GB !" -ForegroundColor Green
} else {
    Write-Host "Khong tim thay file VHDX nao cua Docker de xu ly." -ForegroundColor Red
}

Write-Host "Nhan Enter de thoat..."
Read-Host