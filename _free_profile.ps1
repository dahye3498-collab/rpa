# Free the browser_session profile before capture.
# Chrome cannot open the same user-data-dir twice, so leftover chrome
# (previous run / keep-browser) must be closed or capture fails.
# ASCII only: Windows PowerShell 5.1 mis-reads UTF-8 .ps1 without BOM.
$procs = Get-CimInstance Win32_Process -Filter "Name='chrome.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -like '*browser_session*' }
if ($procs) {
    $procs | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Seconds 1
    Write-Host ("Closed leftover chrome: {0}" -f $procs.Count)
} else {
    Write-Host "No leftover chrome."
}
$lock = Join-Path $PSScriptRoot 'browser_session\SingletonLock'
if (Test-Path $lock) { Remove-Item $lock -Force -ErrorAction SilentlyContinue }
