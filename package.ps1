$pwd = Get-Location
$source = Join-Path $pwd "src"
$destination = Join-Path $pwd "lambda.zip"
Add-Type -assembly "system.io.compression.filesystem"

If (Test-Path $destination) { Remove-Item $destination }

[io.compression.zipfile]::CreateFromDirectory($source, $destination)