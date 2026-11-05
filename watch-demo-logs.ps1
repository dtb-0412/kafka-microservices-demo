# Poll with a timestamp cursor so logs resume after a service stops/starts.
$Host.UI.RawUI.WindowTitle = 'Terminal 2 - Event-Driven Demo Logs'
$cursor = [DateTimeOffset]::UtcNow.ToString('o')
$seen = New-Object 'System.Collections.Generic.HashSet[string]'
Write-Host 'Theo doi Order -> Kafka -> Payment -> Kafka -> Notification' -ForegroundColor Cyan
Write-Host 'Log moi hien o day. Dung/start service o Terminal 1; Ctrl+C de dung theo doi.'
Write-Host 'Order Service' -ForegroundColor Cyan
Write-Host 'Payment Service' -ForegroundColor Yellow
Write-Host 'Notification Service' -ForegroundColor Green

while ($true) {
    $requestStart = [DateTimeOffset]::UtcNow.ToString('o')
    $batch = @(docker compose logs --no-color --timestamps --since $cursor order-service payment-service notification-service)
    if ($LASTEXITCODE -eq 0) {
        foreach ($line in $batch) {
            if ($seen.Add([string]$line)) {
                $serviceColor = switch -Regex ($line) {
                    '^order-service\S*\s+\|' { 'Cyan'; break }
                    '^payment-service\S*\s+\|' { 'Yellow'; break }
                    '^notification-service\S*\s+\|' { 'Green'; break }
                    default { 'Gray' }
                }
                Write-Host $line -ForegroundColor $serviceColor
            }
        }
        # Overlap requests to avoid losing lines written during a fetch.
        # The set suppresses lines already shown in this demo session.
        $cursor = $requestStart
    } else {
        Write-Host 'Chua doc duoc log Docker; se thu lai...' -ForegroundColor Yellow
    }
    Start-Sleep -Seconds 1
}
