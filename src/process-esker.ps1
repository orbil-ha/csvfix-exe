<#
  Synthomer CSV Date Fixer - Batch / Scheduled version
  - Processes all .csv files in INPUT_FOLDER
  - Detects site (PC1/P11), fixes dates in columns 3,4,11,13
  - Outputs synthomer_<SITE>_ESKER_<YYYY-MM-DD>.csv
  - Logs per run; moves source file to processed/failed
#>

param(
  # Optional override of the output date used in the filename (YYYY-MM-DD)
  [string]$OutDate = $(Get-Date -Format 'yyyy-MM-dd')
)

### =========================
### CONFIG
### =========================
$INPUT_FOLDER     = 'C:\Data\ESKER\in'
$OUTPUT_FOLDER    = 'C:\Data\ESKER\out'
$PROCESSED_FOLDER = 'C:\Data\ESKER\processed'
$FAILED_FOLDER    = 'C:\Data\ESKER\failed'
$LOG_FOLDER       = 'C:\Data\ESKER\logs'

# Default site if detection fails
$DEFAULT_SITE     = 'PC1'   # or 'P11'

# Columns to transform (1-based)
$ColsWithTime     = @(3,4)
$ColsDateOnly     = @(11,13)

# File mask
$FILE_MASK        = '*.csv'

### =========================
### PREP
### =========================
$folders = @($INPUT_FOLDER,$OUTPUT_FOLDER,$PROCESSED_FOLDER,$FAILED_FOLDER,$LOG_FOLDER)
$folders | ForEach-Object { if (-not (Test-Path $_)) { New-Item -ItemType Directory -Force -Path $_ | Out-Null } }

$RunId = Get-Date -Format 'yyyyMMdd_HHmmss'
$LogFile = Join-Path $LOG_FOLDER "run_$RunId.log"

function Log($msg) {
  $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
  "$ts  $msg" | Tee-Object -FilePath $LogFile -Append
}

Log "=== Synthomer CSV Date Fixer (Batch) - Start ==="
Log "Input: $INPUT_FOLDER ; Output: $OUTPUT_FOLDER ; Date: $OutDate"

### =========================
### HELPERS
### =========================

# Safe read of first N lines without loading the whole file into memory
function Get-FirstLines($filePath, $n=3) {
  $lines = @()
  $reader = [System.IO.File]::OpenText($filePath)
  try {
    for ($i=0; $i -lt $n; $i++) {
      $line = $reader.ReadLine()
      if ($null -eq $line) { break }
      $lines += $line
    }
  } finally {
    $reader.Close()
  }
  return ,$lines
}

function Detect-Site($filePath) {
  $name = [System.IO.Path]::GetFileName($filePath)

  # 1) Try filename
  if ($name -match '(?i)\bPC1\b') { return 'PC1' }
  if ($name -match '(?i)\bP11\b') { return 'P11' }

  # 2) Try content (first data row)
  $firstLines = Get-FirstLines -filePath $filePath -n 3
  if ($firstLines.Count -ge 2) {
    $header = $firstLines[0]
    $data1  = $firstLines[1]

    # very lightweight split (this is only for detection, not transformation)
    $cols = $data1 -split ','
    $first3 = ($cols | Select-Object -First 3) -join ' '

    if ($first3 -match '(?i)\bPC1\b') { return 'PC1' }
    if ($first3 -match '(?i)\bP11\b') { return 'P11' }
  }

  # 3) Fallback
  return $DEFAULT_SITE
}

# Build regex for a target CSV column (1-based) that may be quoted or not.
# withTime=$true matches DD/MM/YYYY HH:mm:ss, otherwise DD/MM/YYYY
function New-ColumnRegex([int]$colIndex, [bool]$withTime) {
  $before = '^((?:(?:"(?:[^"]|"")*"|[^,]*),){' + ($colIndex - 1) + '})'
  $dd     = '(\d{2})'
  $mm     = '(\d{2})'
  $yyyy   = '(\d{4})'
  $time   = $withTime ? '\s+\d{2}:\d{2}:\d{2}' : ''

  # quoted or unquoted field; capture the whole field as group 2
  $field  = '(' +
              '(?:' +
                '"' + $dd + '\/' + $mm + '\/' + $yyyy + ($withTime ? '(\s+\d{2}:\d{2}:\d{2})?' : '') + '"' +
              '|' +
                $dd + '\/' + $mm + '\/' + $yyyy + ($withTime ? '(\s+\d{2}:\d{2}:\d{2})?' : '') + '(?=,|$)' +
              ')' +
            ')'
  $after  = '((?:,.*)?$)'

  return New-Object System.Text.RegularExpressions.Regex ($before + $field + $after)
}

$ReByCol = @{
  "time" = @{}
  "date" = @{}
}
foreach ($c in $ColsWithTime) { $ReByCol.time[$c] = New-ColumnRegex -colIndex $c -withTime $true }
foreach ($c in $ColsDateOnly) { $ReByCol.date[$c] = New-ColumnRegex -colIndex $c -withTime $false }

function Fix-Line($line, [bool]$isHeader, [ref]$changed, [ref]$unchanged) {
  if ($isHeader) { $unchanged.Value++ ; return $line }

  $original = $line
  $out = $line

  foreach ($kv in $ReByCol.time.GetEnumerator()) {
    $re = $kv.Value
    $out = $re.Replace($out, {
      param($m)
      $g1 = $m.Groups[1].Value
      $g2 = $m.Groups[2].Value
      $g3 = $m.Groups[3].Value

      $m2 = [regex]::Match($g2, '(\d{2})\/(\d{2})\/(\d{4})')
      if (-not $m2.Success) { return $m.Value }
      $d   = $m2.Groups[1].Value
      $mon = $m2.Groups[2].Value
      $y   = $m2.Groups[3].Value
      $iso = "$y-$mon-$d"

      $wasQuoted = $g2.StartsWith('"') -and $g2.EndsWith('"')
      $newField  = $wasQuoted ? ('"'+$iso+'"') : $iso
      return $g1 + $newField + $g3
    })
  }

  foreach ($kv in $ReByCol.date.GetEnumerator()) {
    $re = $kv.Value
    $out = $re.Replace($out, {
      param($m)
      $g1 = $m.Groups[1].Value
      $g2 = $m.Groups[2].Value
      $g3 = $m.Groups[3].Value

      $m2 = [regex]::Match($g2, '(\d{2})\/(\d{2})\/(\d{4})')
      if (-not $m2.Success) { return $m.Value }
      $d   = $m2.Groups[1].Value
      $mon = $m2.Groups[2].Value
      $y   = $m2.Groups[3].Value
      $iso = "$y-$mon-$d"

      $wasQuoted = $g2.StartsWith('"') -and $g2.EndsWith('"')
      $newField  = $wasQuoted ? ('"'+$iso+'"') : $iso
      return $g1 + $newField + $g3
    })
  }

  if ($out -ne $original) { $changed.Value++ } else { $unchanged.Value++ }
  return $out
}

function Process-File($filePath) {
  $site = Detect-Site -filePath $filePath
  Log "Processing: $(Split-Path $filePath -Leaf) | Detected site: $site"

  $tmpOut = [System.IO.Path]::GetTempFileName()
  $reader = [System.IO.File]::OpenText($filePath)
  $writer = New-Object System.IO.StreamWriter($tmpOut, $false, [System.Text.Encoding]::UTF8)

  $lineIndex = 0
  $changed = 0
  $unchanged = 0
  try {
    while ($true) {
      $line = $reader.ReadLine()
      if ($null -eq $line) { break }
      $isHeader = ($lineIndex -eq 0)
      $refChanged = [ref]$changed
      $refUnchanged = [ref]$unchanged
      $fixed = Fix-Line -line $line -isHeader $isHeader -changed $refChanged -unchanged $refUnchanged
      $writer.WriteLine($fixed)
      $lineIndex++
    }
  } finally {
    $reader.Close()
    $writer.Close()
  }

  # Build final output name using supplied OutDate (or today's date)
  $outName = "synthomer_{0}_ESKER_{1}.csv" -f $site, $OutDate
  $destPath = Join-Path $OUTPUT_FOLDER $outName

  # If file exists, add sequence suffix to avoid overwrite
  $seq = 1
  $finalPath = $destPath
  while (Test-Path $finalPath) {
    $name = [System.IO.Path]::GetFileNameWithoutExtension($destPath)
    $ext  = [System.IO.Path]::GetExtension($destPath)
    $finalPath = Join-Path $OUTPUT_FOLDER ("{0}({1}){2}" -f $name, $seq, $ext)
    $seq++
  }

  Move-Item -Path $tmpOut -Destination $finalPath -Force

  Log "Lines total: $lineIndex | Changed: $changed | Unchanged (incl. header): $unchanged"
  Log "Output: $finalPath"
  return @{ Success = $true; Output = $finalPath; Site = $site; Lines = $lineIndex; Changed = $changed; Unchanged = $unchanged }
}

### =========================
### MAIN
### =========================
$files = Get-ChildItem -Path $INPUT_FOLDER -Filter $FILE_MASK -File | Sort-Object LastWriteTime
if ($files.Count -eq 0) {
  Log "No input files found."
} else {
  foreach ($f in $files) {
    try {
      $result = Process-File -filePath $f.FullName
      # Move original
      $target = Join-Path $PROCESSED_FOLDER $f.Name
      Move-Item -Path $f.FullName -Destination $target -Force
      Log "OK → moved original to: $target"
    }
    catch {
      Log "ERROR processing $($f.Name): $($_.Exception.Message)"
      try {
        $target = Join-Path $FAILED_FOLDER $f.Name
        Move-Item -Path $f.FullName -Destination $target -Force
        Log "Moved to FAILED: $target"
      } catch { Log "Secondary move failure: $($_.Exception.Message)" }
    }
  }
}
Log "=== Completed ==="
``
